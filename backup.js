// Copyright © 2017, 2024 IBM Corp. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Small script which backs up a Cloudant or CouchDB database to an S3
// bucket via a stream rather than on-disk file.
//
// The script generates the backup object name by combining together the path
// part of the database URL and the current time.

require("dotenv").config();
const { PassThrough } = require("node:stream");

const { HeadBucketCommand, S3Client } = require("@aws-sdk/client-s3");
const { Upload } = require("@aws-sdk/lib-storage");
const debug = require("debug")("s3-backup");
const VError = require("verror").VError;
const axios = require("axios");
const workerpool = require("workerpool");
const fs = require("node:fs");
const archiver = require("archiver");
const { CronJob } = require("cron");

const pool = new workerpool.pool(__dirname + "/worker.js", { maxWorkers: 4 });

/*
  Main function, run from base of file.
*/
async function main() {
  const startTime = Date.now();
  const sourceUrl = process.env.COUCHDB_SOURCE;
  const backupBucket = process.env.S3_BUCKET;
  const serverName = process.env.SERVER_NAME;

  const dbs = await readAllDb(sourceUrl);

  console.log("Creating backup");
  const filePrefix = `backup-${serverName}`;
  if (!fs.existsSync(filePrefix)) {
    fs.mkdirSync(filePrefix);
  }
  const s3 = new S3Client({
    credentials: {
      accessKeyId: process.env.S3_ACCESS_KEY,
      secretAccessKey: process.env.S3_SECRET_KEY,
    },
    region: process.env.S3_REGION,
  });

  const promises = [];
  dbs.forEach((db) => {
    promises.push(
      pool
        .exec("backupToFile", [`${sourceUrl}/${db}`, db, serverName])
        .then((res) => {
          console.log("Done", db);
        })
        .catch((err) => {}),
    );
  });
  await Promise.all(promises);
  await zipFolder(`./backup-${serverName}`, `./backup-${serverName}.zip`);

  bucketAccessible(s3, backupBucket)
    .then(() => {
      const backupKey = `${process.env.SERVER_NAME}/${Date.now()}.zip`;
      return uploadNewBackup(
        s3,
        `./backup-${serverName}.zip`,
        backupBucket,
        backupKey,
      );
    })
    .then(() => {
      console.log(`Done backup in ${Date.now() - startTime} seconds`);
    })
    .catch((reason) => {
      debug(`Error: ${reason}`);
    });
}

function uploadNewBackup(s3, backupTmpFilePath, bucket, key) {
  console.log(`Uploading from ${backupTmpFilePath} to ${bucket}/${key}`);
  const inputStream = fs.createReadStream(backupTmpFilePath);
  try {
    const upload = new Upload({
      client: s3,
      params: {
        Bucket: bucket,
        Key: key,
        Body: inputStream,
      },
      queueSize: 5, // allow 5 parts at a time
      partSize: 1024 * 1024 * 64, // 64 MB part size
    });
    upload.on("httpUploadProgress", (progress) => {
      console.log(`S3 upload progress: ${JSON.stringify(progress)}`);
    });
    // Return a promise for the completed or aborted upload
    return upload
      .done()
      .finally(() => {
        console.log("S3 upload done");
      })
      .then(() => {
        console.log("Upload succeeded");
      })
      .catch((err) => {
        console.log(err);
        throw new VError(err, "Upload failed");
      });
  } catch (err) {
    debug(err);
    return Promise.reject(new VError(err, "Upload could not start"));
  }
}

async function readAllDb() {
  const response = await axios.get(`${process.env.COUCHDB_SOURCE}/_all_dbs`);
  return response.data;
}

function zipFolder(folderPath, zipFilePath) {
  return new Promise((resolve, reject) => {
    const output = fs.createWriteStream(zipFilePath);
    const archive = archiver("zip", {
      zlib: { level: 9 }, // Set compression level
    });

    output.on("close", () => {
      console.log("Folder has been zipped successfully");
      resolve();
    });

    archive.on("error", (err) => {
      reject(err);
    });

    archive.pipe(output);
    archive.directory(folderPath, false);
    archive.finalize();
  });
}

/**
 * Return a promise that resolves if the bucket is available and
 * rejects if not.
 *
 * @param {any} s3 S3 client object
 * @param {any} bucketName Bucket name
 * @returns Promise
 */
function bucketAccessible(s3, bucketName) {
  console.log("Calling this");
  return s3
    .send(
      new HeadBucketCommand({
        Bucket: bucketName,
      }),
    )
    .catch((e) => {
      throw new VError(e, "S3 bucket not accessible");
    });
}

const job = new CronJob(
  process.env.CRON_TIME, // cronTime
  main, // onTick
  null, // onComplete
  true, // start
  process.env.TIME_ZONE, // timeZone
);
