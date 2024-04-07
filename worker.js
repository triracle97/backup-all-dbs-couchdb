const { backup } = require("@cloudant/couchbackup");
const fs = require("node:fs");
const { VError } = require("verror");
const workerpool = require("workerpool");

function backupToFile(sourceUrl, dbName, serverName) {
  return new Promise((resolve, reject) => {
    backup(
      sourceUrl,
      fs.createWriteStream(`./backup-${serverName}/${dbName}`),
      { mode: 'shallow' },
      (err, done) => {
        if (err) {
          reject(err);
        } else {
          resolve(done);
        }
      },
    )
      .on("changes", (batch) =>
        console.log("Couchbackup changes batch: ", batch),
      )
      .on("written", (progress) =>
        console.log(
          "Fetched batch:",
          progress.batch,
          "Total document revisions written:",
          progress.total,
          "Time:",
          progress.time,
        ),
      );
  })
    .then((done) => {
      console.log(
        `couchbackup download from ${sourceUrl} complete; backed up ${done.total}`,
      );
    })
    .catch((err) => {
      console.log(err);
      throw new VError(err, "couchbackup process failed");
    });
}

workerpool.worker({
  backupToFile: backupToFile,
});
