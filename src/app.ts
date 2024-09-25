import express from "express";
import cors from "cors";
import Bull from "bull";
import dotenv from "dotenv";
import {
  S3Client,
  UploadPartCommand,
  CompleteMultipartUploadCommand,
  ListPartsCommand,
} from "@aws-sdk/client-s3";
import { Part, PayloadData } from "../types";
import { createHash } from "crypto";
import { NodeHttpHandler } from "@smithy/node-http-handler";
import { prisma } from "../prisma/db.setup";

const app = express();
app.use(cors);
const port = 3001;
dotenv.config();
const {
  REDIS_HOST,
  REDIS_PORT,
  BUCKET_NAME: bucketName,
  BUCKET_REGION: bucketRegion,
  ACCESS_KEY: accessKey,
  SECRET_ACCESS_KEY: secretAccessKey,
} = process.env;

const s3v3 = new S3Client({
  credentials: {
    accessKeyId: accessKey!,
    secretAccessKey: secretAccessKey!,
  },
  region: bucketRegion,
  requestHandler: new NodeHttpHandler({ socketTimeout: 1200000 }),
  maxAttempts: 10,
});

if (!REDIS_HOST || !REDIS_PORT) {
  throw new Error("error");
}
const redisOptions = {
  host: REDIS_HOST,
  port: parseInt(REDIS_PORT),
};

const Parts: Part[] = [];
const payloadDataArray: PayloadData[] = [];
let videoTitle = "";

const uploadQueue = new Bull("uploadQueue", { redis: redisOptions });

uploadQueue.process("title", async (payload, done) => {
  videoTitle = payload.data.title;
  done();
});

uploadQueue.process("video_part", async (payload, done) => {
  payload.data = {
    ...payload.data,
    Body: Buffer.from(payload.data.Body.data),
  };

  const hash = createHash("md5").update(payload.data.Body).digest("base64");
  payloadDataArray.push(payload.data);
  done();
});

uploadQueue.process("last_video_part", async (payload, done) => {
  payload.data = {
    ...payload.data,
    Body: Buffer.from(payload.data.Body.data),
  };

  const cloudfrontDistribution = process.env.CLOUDFRONT_DISTRIBUTION;
  const fileName = payload.data.Key;

  payloadDataArray.push(payload.data);

  const promises = payloadDataArray.map((payload) => {
    const uploadPartCommand = new UploadPartCommand(payload);
    return new Promise<void>((resolve, reject) => {
      s3v3
        .send(uploadPartCommand)
        .then((res) => {
          try {
            if (res.ETag) {
              Parts.push({
                ETag: res.ETag,
                PartNumber: payload.PartNumber,
              });
              resolve();
            }
          } catch (error) {
            console.error("Error uploading part: ", error);
            reject(error);
          }
        })
        .catch(reject);
    });
  });

  await Promise.all(promises)
    .then(async () => {
      Parts.sort((a, b) => a.PartNumber - b.PartNumber);
      const listPartsCommand = new ListPartsCommand({
        Bucket: bucketName,
        Key: payload.data.Key,
        UploadId: payload.data.UploadId,
      });
      const partsData = await s3v3.send(listPartsCommand);

      const cmpucParams = {
        Bucket: payload.data.Bucket,
        Key: payload.data.Key,
        MultipartUpload: {
          Parts,
        },
        UploadId: payload.data.UploadId,
      };

      const completeMultipartUploadCommand = new CompleteMultipartUploadCommand(
        cmpucParams
      );

      try {
        const completion = await s3v3
          .send(completeMultipartUploadCommand)
          .then(async (res) => {
            await fetch(
              "https://z89m6eihob.execute-api.us-east-1.amazonaws.com/dev",
              {
                method: "POST",
                headers: {
                  "Content-Type": "application/json",
                },
                body: JSON.stringify({ fileName }),
              }
            ).then(async (res) => {
              const response = await res.json();
            });
            await prisma.video.create({
              data: {
                title: videoTitle,
                filename: fileName.split(".")[0],
              },
            });
          });
        done();
      } catch (error) {
        console.error("Error in CompleteMultipartUploadCommand", error);
        done();
      }
    })
    .catch((error) => console.error("Error during upload", error));
});

app.listen(port, () => {
  console.log(`Server listening on port ${port}`);
});
