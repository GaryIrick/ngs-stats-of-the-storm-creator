const agent = require('superagent')
const delay = require('delay')
const { ngsApiUrl } = require('./config')

module.exports = async (route, payload) => {
  const url = `${ngsApiUrl}/${route}`
  const post = agent
    .post(url)
    .send(payload)

  try {
    await delay(50)
    return (await post).body
  } catch (e) {
    await delay(1000)
    return (await post).body
  }
}
