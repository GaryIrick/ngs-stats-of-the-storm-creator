# ngs-stats-of-the-storm-creator

This repo will:

- Pull down replays from NGS.
- Create a Stats of the Storm database from those replays.
- Zip up the database and upload it to S3.

Learn more about Stats of the Storm [here](https://ebshimizu.github.io/stats-of-the-storm/).

## Setup

You will need to update these values in config.js to match your S3 bucket:

- statsBucket
- statsFolder

## Running

Run `npm install` before you run this for the first time.

Run `node run.js` to update the database. This code is smart enough to keep track of which replays are already part of the database and only add new replays.

The code will kick out messages to the console as it runs so you can view progress and troubleshoot any issues.

## Using the database

Download the .ZIP file from S3. Configure Stats of the Storm to use the directory that contains these files, and you should see the data you expect.
