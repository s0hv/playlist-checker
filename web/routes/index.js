import express from 'express';
import { GetObjectCommand } from '@aws-sdk/client-s3';
import mime from 'mime-types';
import NodeCache from 'node-cache';

import { getFileExtension, parseRange } from '../utils/utilities.js';
import { s3Client } from '../S3/client.js';
import config from '../utils/config.js';
import { ThrottleDownloads } from '../utils/ratelimiter.js';
import { day, hour } from '../utils/units.js';

const router = express.Router();
const cache = new NodeCache({
  stdTTL: hour,
  useClones: false
});

const bucket = process.env.BUCKET;

router.get('/:filename', (req, res, next) => {
  const cachedResponse = cache.get(req.params.filename);
  if (cachedResponse) {
    return res.sendStatus(404);
  }
  const ext = getFileExtension(req.params.filename);
  if (!ext || !config.allowedExtensions.has(ext)) {
    return next();
  }

  let Range;
  if (req.headers.range) {
     const match = parseRange(req.headers.range);
     if (!match) {
       return res.sendStatus(416);
     }
     Range = `bytes=${match.start}-${match.end}`;
  }


  s3Client.send(
    new GetObjectCommand({
      Bucket: bucket,
      Key: req.params.filename,
      Range
    })
  )
    .then(data => {
      const contentType = mime.lookup(ext) || config.mimetypes[ext] || 'application/octet-stream';

      res.setHeader('Content-Type', contentType)
        .setHeader('Content-Length', data.ContentLength)
        .setHeader('Connection', 'keep-alive')
        .setHeader('Content-Transfer-Encoding', 'binary')
        .setHeader('ETag', data.ETag)
        .setHeader('Cache-Control', `max-age=${day*7}`);

      // If specific range was requested set proper headers
      // Reference https://github.com/meloncholy/vid-streamer/blob/master/index.js#L213
      if (req.headers.range) {
        res.setHeader('Content-Range', data.ContentRange)
        .setHeader('Accept-Ranges', 'bytes');
        res.setHeader('Status', '206 Partial Content')
        res.status(206);
      }

      data.Body
        .pipe(new ThrottleDownloads())
        .on('error', () => res.end())
        .pipe(res);
    })
    .catch(err => {
      if (err?.$response?.statusCode === 404) {
        cache.set(req.params.filename, { statusCode: 404 });
        return res.sendStatus(404);
      }

      console.error(err);
      res.status(500).send('Error');
    })
});


export default router
