const fs = require('fs')
const parser = require('hots-parser')
const LinvoDB = require('linvodb3')
const postFromNgs = require('./postFromNgs')
const getFromNgs = require('./getFromNgs')
const { uniq } = require('lodash')
const { replayCachePath, dbPath, currentSeason } = require('./config')

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
      }
      resolve(collections)
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
          }

          collectionMap[collection.name] = collection._id
          resolve(collection)
        })
      })
    }
  }

  return collectionMap
}

const createTeams = async (db, teams) => {
  const requiredTeams = teams.map(t => t.teamName)

  const existingTeams = await new Promise((resolve, reject) => {
    db.settings.find({ type: 'team' }, (err, foundTeams) => {
      if (err) {
        reject(err)
      }
      resolve(foundTeams)
    })
  })

  const teamMap = {}

  for (const existingTeam of existingTeams) {
    teamMap[existingTeam.name] = existingTeam._id
  }

  for (const teamName of requiredTeams) {
    if (!existingTeams.find((t) => t.name === teamName)) {
      await new Promise((resolve, reject) => {
        db.settings.insert({ type: 'team', name: teamName, players: [] }, (err, createdTeam) => {
          if (err) {
            reject(err)
          }

          teamMap[createdTeam.name] = createdTeam._id
          resolve(createdTeam)
        })
      })
    }
  }

  return teamMap
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
        }

        resolve(newDoc)
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
        }

        resolve()
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
          }

          resolve()
        })
    })
  }
}

const run = async () => {
  const db = openDatabase(dbPath)
  const { returnObject: matches } = await postFromNgs('schedule/fetch/reported/matches', { season: currentSeason })
  const { returnObject: teams } = await getFromNgs('team/get/registered')

  const collectionMap = await createCollections(db, teams)
  const teamMap = await createTeams(db, teams)

  const files = fs.readdirSync(replayCachePath)

  for (const file of files) {
    const { match, players, status } = parser.processReplay(`${replayCachePath}//${file}`, { overrideVerifiedBuild: true })

    if (status === 1) {
      // eslint-disable-next-line no-await-in-loop
      const matchID = await insertReplay(db, match, players, [])

      if (matchID) {
        // eslint-disable-next-line no-await-in-loop
        await updatePlayers(db, players)
      }

      console.log('parsed ' + file)
    } else {
      console.log(`status = ${status}`)
    }
  }
}

run().then(() => console.log('done'))
