const syncDatabase = require('./syncDatabase')

syncDatabase().then(() => console.log('Complete.'))
