import pino from 'pino';
import * as path from 'path';

const loggerFormat = process.env.EXPRESS_LOG_FORMAT || "{res.statusCode} {req.method} {responseTime}ms {ip} {urlDecoded} {err.stack}";

const customFormat = (info) => {
  if (info instanceof Error || info.stack) {
    return `${info.timestamp} [${info.level}]: ${info.message} ${info.stack}`;
  }
  return `${info.timestamp} [${info.level}]: ${info.message}`;
}

export const getTransports = () => {
  /** @type {(pino.TransportPipelineOptions | pino.TransportTargetOptions)[]} */
  const targets = [];

  /** @type {pino.TransportTargetOptions} */
  const pinoPrettyConfig = {
    target: 'pino-pretty',
    options: {
      colorize: true,
      messageFormat: loggerFormat,
      translateTime: 'yyyy-mm-dd HH:MM:ss.l',
      hideObject: true,
    },
  }

  if (process.env.NODE_ENV !== 'production') {
    targets.push({
      ...pinoPrettyConfig,
      level: 'debug',
    })
  }

  const filename = path.join(process.env.LOGS_DIR || '', 'server.log');
  targets.push({
    ...pinoPrettyConfig,
    options: {
      ...pinoPrettyConfig.options,
      level: 'info',
      destination: filename,
    },
  })

  return pino.transport({
    targets,
    sync: true,
  })
}

export const logger = pino(getTransports());
