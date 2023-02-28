const fs = require('fs')
const agent = require('superagent')
const parser = require('hots-parser')
const LinvoDB = require('linvodb3')
const postFromNgs = require('./postFromNgs')
const getFromNgs = require('./getFromNgs')
const { uniq, startCase } = require('lodash')
const { replayCachePath, dbPath, currentSeason, ngsBucket } = require('./config')

LinvoDB.defaults.store = { db: require('medeadown') }

const openDatabase = (path) => {
  const db = {}

  db.matches = new LinvoDB('matches', {}, { filename: path + '/matches.ldb' })
  db.heroData = new LinvoDB('heroData', {}, { filename: path + '/hero.ldb' })
  db.players = new LinvoDB('players', {}, { filename: path + '/players.ldb' })
  db.settings = new LinvoDB('settings', {}, { filename: path + '/settings.ldb' })

  db.matches.ensureIndex({ fieldName: 'map' })
  db.players.ensureIndex({ fieldName: 'hero' })

  return db
}

const createCollections = async (db, teams) => {
  const requiredCollections = uniq(teams.map(t => t.divisionDisplayName))

  for (const coastalDivision of requiredCollections.filter(d => d.includes(' '))) {
    requiredCollections.push(coastalDivision.split(' ')[0])
  }

  requiredCollections.push('Non-Storm')

  const existingCollections = await new Promise((resolve, reject) => {
    db.settings.find({ type: 'collection' }, (err, collections) => {
      if (err) {
        reject(err)
      } else {
        resolve(collections)
      }
    })
  })

  const collectionMap = {}

  for (const existingCollection of existingCollections) {
    collectionMap[existingCollection.name] = existingCollection._id
  }

  for (const collection of uniq(requiredCollections)) {
    if (!existingCollections.find((c) => c.name === collection)) {
      await new Promise((resolve, reject) => {
        db.settings.insert({ type: 'collection', name: collection }, (err, collection) => {
          if (err) {
            reject(err)
          } else {
            collectionMap[collection.name] = collection._id
            resolve(collection)
          }
        })
      })
    }
  }

  return collectionMap
}

const createTeams = async (db, requiredTeams) => {
  const existingTeams = await new Promise((resolve, reject) => {
    db.settings.find({ type: 'team' }, (err, foundTeams) => {
      if (err) {
        reject(err)
      } else {
        resolve(foundTeams)
      }
    })
  })

  const teamMap = {}

  for (const existingTeam of existingTeams) {
    teamMap[existingTeam.name] = { id: existingTeam._id, name: existingTeam.name, players: [] }
  }

  for (const requiredTeam of requiredTeams) {
    if (!existingTeams.find((t) => t.name === requiredTeam.teamName)) {
      await new Promise((resolve, reject) => {
        db.settings.insert({ type: 'team', name: requiredTeam.teamName, players: [] }, (err, createdTeam) => {
          if (err) {
            reject(err)
          } else {
            teamMap[createdTeam.name] = { id: createdTeam._id, name: requiredTeam.teamName, players: [] }
            resolve(createdTeam)
          }
        })
      })
    }

    // Add in all the rostered players.  We might add players below if someone subbed
    // for this team.
    const players = teamMap[requiredTeam.teamName].players

    for (const teamMember of requiredTeam.teamMembers) {
      players.push(teamMember.displayName)
    }
  }

  return teamMap
}

const findMatchingTeam = (homeTeam, awayTeam, players) => {
  let homeCount = 0
  let awayCount = 0

  for (const player of players) {
    if (homeTeam && homeTeam.players.includes(player)) {
      homeCount++
    }

    if (awayTeam && awayTeam.players.includes(player)) {
      awayCount++
    }
  }

  if (homeCount > 2 && homeCount > awayCount) {
    return homeTeam
  } else if (awayCount > 2 && awayCount > homeCount) {
    return awayTeam
  } else {
    // This can happen when a team withdraws
    return undefined
  }
}

const insertReplay = async (db, match, players, collections) => {
  if (!collections) {
    match.collection = []
  } else {
    match.collection = collections
  }

  const createdMatch = await new Promise((resolve, reject) => {
    db.matches.update(
      { map: match.map, date: match.date, type: match.type },
      match,
      { upsert: true },
      (err, numReplaced, newDoc) => {
        if (err) {
          reject(err)
        } else {
          resolve(newDoc)
        }
      })
  })

  if (createdMatch) {
    const playerArray = []

    for (const i in players) {
      players[i].matchID = createdMatch._id
      players[i].collection = createdMatch.collection
      playerArray.push(players[i])
    }

    await new Promise((resolve, reject) => {
      db.heroData.insert(playerArray, function (err, docs) {
        if (err) {
          reject(err)
        } else {
          resolve()
        }
      })
    })
  }

  return createdMatch ? createdMatch._id : undefined
}

const updatePlayers = async (db, players) => {
  for (const i in players) {
    // log unique players in the player database
    const playerDbEntry = {}
    playerDbEntry._id = players[i].ToonHandle
    playerDbEntry.name = players[i].name
    playerDbEntry.uuid = players[i].uuid
    playerDbEntry.region = players[i].region
    playerDbEntry.realm = players[i].realm

    // in general this will ensure the most recent tag gets associated with each player
    playerDbEntry.tag = players[i].tag

    const updateEntry = { $set: playerDbEntry, $inc: { matches: 1 } }

    await new Promise((resolve, reject) => {
      db.players.update(
        { _id: playerDbEntry._id },
        updateEntry,
        { upsert: true },
        (err, numReplaced, upsert) => {
          if (err) {
            reject(err)
          } else {
            resolve()
          }
        })
    })
  }
}

const downloadFile = async (url, path) => {
  await new Promise((resolve, reject) => {
    const stream = fs.createWriteStream(path)
    agent.get(url).pipe(stream)
    stream.on('finish', resolve)
  })
}

const getCollectionIdsForDivision = (collectionMap, divisionConcat) => {
  const collectionIds = []
  const division = startCase(divisionConcat.replace('-', ' '))
  collectionIds.push(collectionMap[division])

  if (division.includes(' ')) {
    collectionIds.push(collectionMap[division.split(' ')[0]])
  }

  if (division !== 'Storm') {
    collectionIds.push(collectionMap['Non-Storm'])
  }

  return collectionIds
}

const run = async () => {
  const processedMarkerFiles = []
  const db = openDatabase(dbPath)
  const { returnObject: matches } = await postFromNgs('schedule/fetch/reported/matches', { season: currentSeason })
  const { returnObject: teams } = await getFromNgs('team/get/registered')

  const collectionMap = await createCollections(db, teams)
  const teamMap = await createTeams(db, teams)
  const playerMap = {}
  const playersForTeamMap = {}

  for (const teamName in teamMap) {
    playersForTeamMap[teamName] = []
  }

  for (const match of matches) {
    if (match.replays) {
      for (const i in match.replays) {
        if (i === '_id') {
          continue
        }

        const filename = match.replays[i].url

        if (!filename) {
          // This replay is not here, skip it.  This should not happen.
          continue
        }

        const fullUrl = `https://s3.amazonaws.com/${ngsBucket}/${filename}`
        const replayDirectory = `${replayCachePath}/${currentSeason}/${match.divisionConcat}`
        fs.mkdirSync(replayDirectory, { recursive: true })
        const localFile = `${replayDirectory}/${filename}`
        const processedMarkerFile = `${localFile}.processed`

        if (fs.existsSync(processedMarkerFile)) {
          // We've already processed this replay completely, ignore it.
          console.log(`Skipping ${localFile}, was already processed.`)
          continue
        }

        processedMarkerFiles.push(processedMarkerFile)
        await downloadFile(fullUrl, localFile)
        const { match: replay, players, status } = parser.processReplay(localFile, { overrideVerifiedBuild: true })
        const bluePlayers = []
        const redPlayers = []

        for (const toonHandle in players) {
          const player = players[toonHandle]
          const tag = `${player.name}#${player.tag}`

          if (player.team === 0) {
            bluePlayers.push(tag)
          } else {
            redPlayers.push(tag)
          }
        }

        const homeTeam = teamMap[match.home.teamName]
        const awayTeam = teamMap[match.away.teamName]

        const blueTeam = findMatchingTeam(homeTeam, awayTeam, bluePlayers)
        const redTeam = findMatchingTeam(homeTeam, awayTeam, redPlayers)

        // If a team has withdrawn, we won't find it, so don't try to
        // find ORS that need to be added.
        if (blueTeam) {
          for (const bluePlayer of bluePlayers) {
            if (!blueTeam.players.includes(bluePlayer)) {
              blueTeam.players.push(bluePlayer)
              console.log(`Adding ORS ${bluePlayer} to ${blueTeam.name}`)
            }
          }
        }

        if (redTeam) {
          for (const redPlayer of redPlayers) {
            if (!redTeam.players.includes(redPlayer)) {
              redTeam.players.push(redPlayer)
              console.log(`Adding ORS ${redPlayer} to ${redTeam.name}`)
            }
          }
        }

        for (const playerID in players) {
          const fullTag = `${players[playerID].name}#${players[playerID].tag}`
          playerMap[fullTag] = playerID
        }

        if (status === 1) {
          const collectionIds = getCollectionIdsForDivision(collectionMap, match.divisionConcat)
          const matchID = await insertReplay(db, replay, players, collectionIds)

          if (matchID) {
            console.log(`Imported ${localFile}.`)
            await updatePlayers(db, players)
          } else {
            console.log(`Skipped ${localFile}, status is ${status}.`)
          }
        }
      }
    }
  }

  for (const team of teams) {
    const toonHandles = []

    for (const playerTag of teamMap[team.teamName].players) {
      if (playerMap[playerTag]) {
        // We can only add players that we saw in a replay,
        // since we need their toon handle.
        toonHandles.push(playerMap[playerTag])
      }
    }

    for (const toonHandle of toonHandles) {
      await new Promise((resolve, reject) => {
        db.settings.update(
          { _id: teamMap[team.teamName].id },
          { $addToSet: { players: toonHandle } },
          {},
          (err, replaced, updatedDoc) => {
            if (err) {
              reject(err)
            } else {
              resolve()
            }
          })
      })
    }
  }

  // Now that we've processed all of these replays and updated the players mentnioned
  // in them, mark them so we skip them on the next run.
  for (const processedMarkerFile of processedMarkerFiles) {
    fs.writeFileSync(processedMarkerFile, 'done')
  }
}

run().then(() => console.log('Complete.'))
