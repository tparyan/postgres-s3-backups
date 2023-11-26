import { exec } from "child_process";
import { PutObjectCommand, S3Client, S3ClientConfig } from "@aws-sdk/client-s3";
import { createReadStream, unlink, statSync } from "fs";
import { filesize } from "filesize";
import path from "path";
import os from "os";

import { env } from "./env";

const uploadToS3 = async ({ name, path }: { name: string, path: string }) => {
  console.log("Uploading backup to S3...");

  const bucket = env.AWS_S3_BUCKET;

  const clientOptions: S3ClientConfig = {
    region: env.AWS_S3_REGION
  }

  if (env.AWS_S3_ENDPOINT) {
    console.log(`Using custom endpoint: ${env.AWS_S3_ENDPOINT}`)
    clientOptions['endpoint'] = env.AWS_S3_ENDPOINT;
  }

  const client = new S3Client(clientOptions);

  await client.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: name,
      Body: createReadStream(path),
    })
  );

  console.log("Backup uploaded to S3...");
}

const dumpToFile = async (path: string) => {
  console.log("Dumping DB to file...");

  await new Promise((resolve, reject) => {
    exec(`pg_dump --dbname=${env.BACKUP_DATABASE_URL} --format=tar | gzip > ${path}`, (error, stdout, stderr) => {
      if (error) {
        reject({ error: error, stderr: stderr.trimEnd() });
        return;
      }

      if (stderr != "") {
        reject({ stderr: stderr.trimEnd() });
        return;
      }

      console.log("Backup size:", filesize(statSync(path).size));

      resolve(undefined);
    });

  });

  console.log("DB dumped to file...");
}

const deleteFile = async (path: string) => {
  console.log("Deleting file...");
  await new Promise((resolve, reject) => {
    unlink(path, (err) => {
      reject({ error: err });
      return;
    });
    resolve(undefined);
  });
}
const downloadFile = async (url: string, outputPath: string): Promise<void> => {
  console.log(`Downloading file from ${url}...`);

  const writer = createWriteStream(outputPath);

  return axios({
    method: 'get',
    url: url,
    responseType: 'stream',
  }).then(response => {
    return new Promise((resolve, reject) => {
      response.data.pipe(writer);
      let error: any = null;
      writer.on('error', err => {
        error = err;
        writer.close();
        reject(err);
      });
      writer.on('close', () => {
        if (!error) {
          console.log(`Downloaded file to ${outputPath}`);
          resolve();
        }
      });
    });
  });
};

export const backup = async () => {
  console.log("Initiating backup...");

  let date = new Date().toISOString();
  const timestamp = date.replace(/[:.]+/g, '-');
  
  // Backup database
  const dbFilename = `backup-db-${timestamp}.tar.gz`;
  const dbFilepath = path.join(os.tmpdir(), dbFilename);
  await dumpToFile(dbFilepath);
  await uploadToS3({ name: dbFilename, path: dbFilepath });
  await deleteFile(dbFilepath);

  // Download and backup zip file from URL
  const zipFilename = `backup-${timestamp}.zip`;
  const zipFilepath = path.join(os.tmpdir(), zipFilename);
  await downloadFile(env.BACKUP_VOLUME_URL, zipFilepath);
  await uploadToS3({ name: zipFilename, path: zipFilepath });
  await deleteFile(zipFilepath);

  console.log("Backup complete...");
}
// export const backup = async () => {
//   console.log("Initiating DB backup...");

//   let date = new Date().toISOString();
//   const timestamp = date.replace(/[:.]+/g, '-');
//   const filename = `backup-${timestamp}.tar.gz`;
//   const filepath = path.join(os.tmpdir(), filename);

//   await dumpToFile(filepath);
//   await uploadToS3({ name: filename, path: filepath });
//   await deleteFile(filepath);

//   console.log("DB backup complete...");
// }
