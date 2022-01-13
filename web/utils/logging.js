import { createLogger, format, transports } from 'winston';
import * as path from 'path';

const customFormat = (info) => {
  if (info instanceof Error || info.stack) {
    return `${info.timestamp} [${info.level}]: ${info.message} ${info.stack}`;
  }
  return `${info.timestamp} [${info.level}]: ${info.message}`;
}

export const getTransports = () => {
  const transports_ = [];
  const logFormat = format.combine(
    format.errors({ stack: true }),
    format.timestamp({
      format: 'YYYY-MM-DD HH:mm:ss.ms'
    }),
    format.colorize(),
    format.printf(customFormat)
  );

  if (process.env.NODE_ENV !== 'production') {
    transports_.push(
      new transports.Console({
        level: 'debug',
        format: logFormat
      })
    )
  }

  const filename = path.join(process.env.LOGS_DIR || '', 'server.log');
  transports_.push(
    new transports.File({
      filename,
      level: 'info',
      format: logFormat
    })
  )

  return transports_;
}

export const logger = createLogger({
  transports: getTransports(),
  handleExceptions: true
});
