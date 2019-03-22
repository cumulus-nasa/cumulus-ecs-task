/* eslint max-len: "off" */

'use strict';

const https = require('https');
const isBoolean = require('lodash.isboolean');
const isObject = require('lodash.isobject');
const isString = require('lodash.isstring');
const path = require('path');
const execSync = require('child_process').execSync;
const assert = require('assert');
const pRetry = require('p-retry');

const AWS = require('aws-sdk');
const fs = require('fs');

const Logger = require('./Logger');
const log = new Logger();

const region = process.env.AWS_DEFAULT_REGION || 'us-east-1';
AWS.config.update({ region: region });

const isLambdaFunctionArn = (id) => id.startsWith('arn:aws:lambda');

function getFunctionName(lambdaId) {
  if (isLambdaFunctionArn(lambdaId)) {
    const FUNCTION_NAME_FIELD = 6;

    return lambdaId.split(':')[FUNCTION_NAME_FIELD];
  }

  return lambdaId;
}

const getLogSenderFromLambdaId = (lambdaId) =>
  `cumulus-ecs-task/${getFunctionName(lambdaId)}`;

/**
 * Download a URL to file
 *
 * @param {string} url - the URL to fetch
 * @param {string} destinationFilename - the filename to write the file to
 * @returns {Promise<undefined>} resolves when file has been downloaded
 */
function tryToDownloadFile(url, destinationFilename) {
  const file = fs.createWriteStream(destinationFilename);

  return new Promise((resolve, reject) => {
    file.on('error', reject);
    file.on('finish', () => file.close());
    file.on('close', resolve);

    return https
      .get(url, (res) => res.pipe(file))
      .on('error', reject);
  });
}

/**
 * Download a URL and save it to a file.  If an ETIMEDOUT error is received,
 * retry the download with an incremental backoff.
 *
 * @param {string} url - the URL to fetch
 * @param {string} destinationFilename - the filename to write the file to
 * @returns {Promise<undefined>} resolves when file has been downloaded
 */
function downloadFile(url, destinationFilename) {
  return pRetry(() =>
    tryToDownloadFile(url, destinationFilename)
      .catch((err) => {
        if (err.code === 'ETIMEDOUT') {
          throw err;
        }

        throw new pRetry.AbortError(err);
      }));
}

/**
* Download the zip file of a lambda function from AWS
*
* @param {string} arn - the arn of the lambda function
* @param {strind} workDir - the dir to download the lambda function to
* @returns {Promise<Object>} returns an object that includes `filepath`,
* `moduleFileName`, `moduleFunctionName` arguments.
* The `filepath` is the path to the zip file of the lambda function.
* The `moduleFileName` is the filename of the node module.
* The `moduleFunctionName` is the name of the exported function to call in the module.
**/
async function getLambdaZip(arn, workDir) {
  const lambda = new AWS.Lambda({ apiVersion: '2015-03-31' });

  const data = await lambda.getFunction({ FunctionName: arn }).promise();

  const codeUrl = data.Code.Location;
  const handlerId = data.Configuration.Handler;
  const moduleFn = handlerId.split('.');
  const moduleFileName = moduleFn[0];
  const moduleFunctionName = moduleFn[1];

  const filepath = path.join(workDir, 'fn.zip');

  await downloadFile(codeUrl, filepath);

  return {
    filepath,
    moduleFileName,
    moduleFunctionName
  };
}

/**
* Downloads and extracts the code of a lambda function from its zip file
*
* @param {string} lambdaArn - the arn of the lambda function
* @param {string} workDir - the temporary dir used to download the lambda zip file
* @param {string} taskDir - the dir where the lambda function will be located
* @returns {Promise<Function>} the `handler` which is the javascript function
*                              that will run in the ECS service
**/
async function downloadLambdaHandler(lambdaArn, workDir, taskDir) {
  const resp = await getLambdaZip(lambdaArn, workDir);

  execSync(`unzip -o ${resp.filepath} -d ${taskDir}`);
  // eslint-disable-next-line import/no-dynamic-require
  const task = require(`${taskDir}/${resp.moduleFileName}`); //eslint-disable-line global-require
  return task[resp.moduleFunctionName];
}

/**
* Starts heartbeat to indicate worker is working on the task
*
* @param {string} taskToken - the task token
* @returns {intervalId} - interval id used by `clearInterval`
**/
function startHeartbeat(taskToken) {
  const sf = new AWS.StepFunctions({ apiVersion: '2016-11-23' });
  return setInterval(() => {
    sf.sendTaskHeartbeat({ taskToken }, (err) => {
      if (err) {
        log.error('error sending heartbeat', err);
      }
      log.info(`sending heartbeat, confirming ${taskToken} is still in progress`);
    }, 60000);
  });
}

/**
* Tells workflow that the task has failed
*
* @param {string} taskToken - the task token
* @param {Object} taskError - the error object returned by the handler
* @returns {Promise<Object>} - step function send task failure output
**/
function sendTaskFailure(taskToken, taskError) {
  const sf = new AWS.StepFunctions({ apiVersion: '2016-11-23' });
  return sf.sendTaskFailure({
    taskToken: taskToken,
    error: taskError.name,
    cause: taskError.message
  }).promise();
}

/**
* Tells workflow that the task has succeeded and provides message for next task
*
* @param {string} taskToken - the task token
* @param {Object} output - output message for next task
* @returns {Promise<Object>} - step function send task success output
**/
function sendTaskSuccess(taskToken, output) {
  const sf = new AWS.StepFunctions({ apiVersion: '2016-11-23' });
  return sf.sendTaskSuccess({
    taskToken: taskToken,
    output: output
  }).promise();
}

/**
 * receives an activity message from the StepFunction Activity Queue
 *
 * @param {string} activityArn - the activity arn
 * @returns {Promise<Object|null>} the lambda task event object and the activity
 *   task's token. If the activity task return empty, the function returns null
 */
async function getActivityTask(activityArn) {
  const sf = new AWS.StepFunctions({ apiVersion: '2016-11-23' });
  const data = await sf.getActivityTask({ activityArn }).promise();

  if (data.taskToken.length > 0 && data.input) {
    const token = data.taskToken;
    const event = JSON.parse(data.input);
    return {
      event,
      token
    };
  }
  log.info('No tasks in the activity queue');

  return null;
}


/**
* Handle the lambda task response
*
* @param {Object} event - the event to pass to the lambda function
* @param {Function} handler - the lambda function to execute
* @returns {Promise} the lambda functions response
**/
function handleResponse(event, handler) {
  const context = { via: 'ECS' };

  return new Promise((resolve, reject) => {
    handler(event, context, (err, output) => {
      if (err) {
        return reject(err);
      }
      return resolve(output);
    });
  });
}

/**
* Handle the data event from poll.getTask()
*
* @param {Object} event - the event to pass to the lambda function
* @param {string} taskToken - the task token
* @param {function} handler - the lambda function to execute
* @param {integer} heartbeatInterval - number of milliseconds between heartbeat messages.
* defaults to null, which deactivates heartbeats
* @returns {undefined} - no return value
**/
async function handlePollResponse(event, taskToken, handler, heartbeatInterval) {
  let heartbeat;

  if (heartbeatInterval) {
    heartbeat = startHeartbeat(taskToken);
  }

  try {
    const output = await handleResponse(event, handler);
    if (heartbeatInterval) {
      clearInterval(heartbeat);
    }
    await sendTaskSuccess(taskToken, JSON.stringify(output));
  }
  catch (err) {
    await sendTaskFailure(taskToken, err);
  }
}

/**
* Start the Lambda handler as a one time task. When the task completes
* the process exits
*
* @param {Object} options - options object
* @param {string} options.lambdaArn - the arn of the lambda handler
* @param {string} options.lambdaInput - the input to the lambda handler
* @param {string} options.taskDirectory - the directory to put the unzipped lambda zip
* @param {string} options.workDirectory - the directory to use for downloading the lambda zip file
* @returns {Promise} the output of the lambda function response
**/
async function runTask(options) {
  assert(options && typeof options === 'object', 'options.lambdaArn string is required');
  assert(options && typeof options.lambdaInput === 'object', 'options.lambdaInput object is required');
  assert(options.taskDirectory && typeof options.taskDirectory === 'string', 'options.taskDirectory string is required');
  assert(options.workDirectory && typeof options.workDirectory === 'string', 'options.workDirectory string is required');

  const lambdaArn = options.lambdaArn;
  const event = options.lambdaInput;
  const taskDir = options.taskDirectory;
  const workDir = options.workDirectory;

  log.sender = getLogSenderFromLambdaId(lambdaArn);

  // the cumulus-message-adapter dir is in an unexpected place,
  // so tell the adapter where to find it
  process.env.CUMULUS_MESSAGE_ADAPTER_DIR = `${taskDir}/cumulus-message-adapter/`;

  log.info('Downloading the Lambda function');
  try {
    const handler = await downloadLambdaHandler(lambdaArn, workDir, taskDir);
    const output = await handleResponse(event, handler);
    log.info('task executed successfully');
    return output;
  }
  catch (e) {
    log.error('task failed with an error', e);
    throw e;
  }
}

/**
* Start the Lambda handler as a service by polling a sqs queue
* The function will not quit unless the process is terminated
*
* @param {Object} options - options object
* @param {string} options.lambdaArn - the arn of the lambda handler
* @param {string} options.sqsUrl   - the url to the sqs queue
* @param {integer} options.heartbeat - number of milliseconds between heartbeat messages.
* defaults to null, which deactivates heartbeats
* @param {string} options.taskDirectory - the directory to put the unzipped lambda zip
* @param {string} options.workDirectory - the directory to use for downloading the lambda zip file
* @param {boolean} [options.runForever=true] - whether to poll the activity forever (defaults to true)
* @returns {Promise<undefined>} undefined
**/
async function runServiceFromSQS(options) {
  assert(isObject(options), 'options.lambdaArn string is required');
  assert(isString(options.lambdaArn), 'options.lambdaArn string is required');
  assert(isString(options.sqsUrl), 'options.sqsUrl string is required');
  assert(isString(options.taskDirectory), 'options.taskDirectory string is required');
  assert(isString(options.workDirectory), 'options.workDirectory string is required');

  const sqs = new AWS.SQS({ apiVersion: '2016-11-23' });

  const lambdaArn = options.lambdaArn;
  const sqsUrl = options.sqsUrl;
  const taskDirectory = options.taskDirectory;
  const workDirectory = options.workDirectory;

  const runForever = isBoolean(options.runForever) ? options.runForever : true;

  log.sender = getLogSenderFromLambdaId(lambdaArn);

  // the cumulus-message-adapter dir is in an unexpected place,
  // so tell the adapter where to find it
  process.env.CUMULUS_MESSAGE_ADAPTER_DIR = `${taskDirectory}/cumulus-message-adapter/`;

  log.info('Downloading the Lambda function');
  const handler = await downloadLambdaHandler(lambdaArn, workDirectory, taskDirectory);

  let sigTermReceived = false;
  process.on('SIGTERM', () => {
    log.info('Received SIGTERM signal');
    sigTermReceived = true;
  });

  /* eslint-disable no-await-in-loop */
  do {
    try {
      const resp = await sqs.receiveMessage({
        QueueUrl: sqsUrl,
        WaitTimeSeconds: 20
      }).promise();
      const messages = resp.Messages;
      if (messages) {
        const promises = messages.map(async (message) => {
          if (message && message.Body) {
            const receipt = message.ReceiptHandle;
            log.info('received message from queue, executing the task');
            const event = JSON.parse(message.Body);
            await handleResponse(event, handler);

            // remove the message from queue
            log.info(`message with handler ${receipt} deleted from the queue`);
            await sqs.deleteMessage({ QueueUrl: sqsUrl, ReceiptHandle: receipt }).promise();
          }
          return undefined;
        });
        await promises;
      }
      else {
        log.info('There are no new messages in the queue. Polling again!');
      }
    }
    catch (e) {
      log.error('Task failed. trying again', e);
    }
  } while (runForever && !sigTermReceived);
  /* eslint-enable no-await-in-loop */
}

/**
* Start the Lambda handler as a service by polling a SF activity queue
* The function will not quit unless the process is terminated
*
* @param {Object} options - options object
* @param {string} options.lambdaArn - the arn of the lambda handler
* @param {string} options.activityArn - the arn of the activity
* @param {integer} options.heartbeat - number of milliseconds between heartbeat messages.
* defaults to null, which deactivates heartbeats
* @param {string} options.taskDirectory - the directory to put the unzipped lambda zip
* @param {string} options.workDirectory - the directory to use for downloading the lambda zip file
* @param {boolean} [options.runForever=true] - whether to poll the activity forever (defaults to true)
* @returns {Promise<undefined>} undefined
**/
async function runServiceFromActivity(options = {}) {
  assert(isObject(options), 'options.lambdaArn string is required');
  assert(isString(options.lambdaArn), 'options.lambdaArn string is required');
  assert(isString(options.activityArn), 'options.activityArn string is required');
  assert(isString(options.taskDirectory), 'options.taskDirectory string is required');
  assert(isString(options.workDirectory), 'options.workDirectory string is required');
  if (options.heartbeat) {
    assert(Number.isInteger(options.heartbeat), 'options.heartbeat must be an integer');
  }

  const runForever = isBoolean(options.runForever) ? options.runForever : true;

  log.sender = getLogSenderFromLambdaId(options.lambdaArn);

  // the cumulus-message-adapter dir is in an unexpected place,
  // so tell the adapter where to find it
  process.env.CUMULUS_MESSAGE_ADAPTER_DIR = `${options.taskDirectory}/cumulus-message-adapter/`;

  log.info('Downloading the Lambda function');
  const handler = await downloadLambdaHandler(
    options.lambdaArn,
    options.workDirectory,
    options.taskDirectory
  );

  let sigTermReceived = false;
  process.on('SIGTERM', () => {
    log.info('Received SIGTERM signal');
    sigTermReceived = true;
  });

  /* eslint-disable no-await-in-loop */
  do {
    const task = await getActivityTask(options.activityArn);

    if (task !== null) {
      try {
        await handlePollResponse(
          task.event,
          task.token,
          handler,
          options.heartbeat
        );
      }
      catch (err) {
        await sendTaskFailure(task.token, err);
      }
    }
  } while (runForever && !sigTermReceived);
  /* eslint-enable no-await-in-loop */
}

module.exports = {
  runServiceFromActivity,
  runServiceFromSQS,
  runTask
};
