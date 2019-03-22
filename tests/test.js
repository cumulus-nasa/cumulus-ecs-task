'use strict';

const archiver = require('archiver');
const AWS = require('aws-sdk');
const fs = require('fs-extra');
const nock = require('nock');
const os = require('os');
const path = require('path');
const sinon = require('sinon');
const test = require('ava');
const { runTask, runServiceFromActivity } = require('..');

test.beforeEach(async (t) => {
  t.context.tempDir = path.join(os.tmpdir(), 'cumulus-ecs-task', `${Date.now()}`, path.sep);
  fs.mkdirpSync(t.context.tempDir);
  t.context.lambdaZip = path.join(t.context.tempDir, 'remoteLambda.zip');
  t.context.taskDirectory = path.join(t.context.tempDir, 'task');
  t.context.workDirectory = path.join(t.context.tempDir, '.tmp-work');
  fs.mkdirpSync(t.context.taskDirectory);
  fs.mkdirpSync(t.context.workDirectory);

  // zip fake lambda
  await new Promise((resolve, reject) => {
    const output = fs.createWriteStream(t.context.lambdaZip);
    const archive = archiver('zip');
    output.on('close', resolve);
    output.on('error', reject);
    archive.pipe(output);
    archive.file(path.join(__dirname, 'data/fakeLambda.js'), { name: 'fakeLambda.js' });
    archive.finalize();
  });

  t.context.lambdaZipUrlPath = '/lambda';

  nock('https://example.com')
    .get(t.context.lambdaZipUrlPath)
    .reply(200, () => fs.createReadStream(t.context.lambdaZip));

  t.context.expectedOutput = [
    'fakeLambda',
    'handler'
  ];
  t.context.stub = sinon.stub(AWS, 'Lambda')
    .returns({
      getFunction: () => ({
        promise: async () => ({
          Code: {
            Location: `https://example.com${t.context.lambdaZipUrlPath}`
          },
          Configuration: {
            Handler: t.context.expectedOutput.join('.')
          }
        })
      })
    });
});

test.afterEach.always((t) => {
  nock.cleanAll();
  t.context.stub.restore();
  fs.removeSync(t.context.tempDir);
});

test.serial('test successful task run', async (t) => {
  const event = { hi: 'bye' };

  const output = await runTask({
    lambdaArn: 'arn:aws:lambda:region:account-id:function:fake-function',
    lambdaInput: event,
    taskDirectory: t.context.taskDirectory,
    workDirectory: t.context.workDirectory
  });

  t.deepEqual(event, output);
});

test.serial('test failed task run', async (t) => {
  const event = { hi: 'bye', error: 'it failed' };
  const promise = runTask({
    lambdaArn: 'arn:aws:lambda:region:account-id:function:fake-function',
    lambdaInput: event,
    taskDirectory: t.context.taskDirectory,
    workDirectory: t.context.workDirectory
  });
  const error = await t.throws(promise);
  t.is(error, event.error);
});

test.serial('test activity success', async (t) => {
  const input = {
    msg: 'this was a success'
  };
  const token = 'some token';

  const sf = sinon.stub(AWS, 'StepFunctions')
    .returns({
      getActivityTask: () => ({
        promise: async () => ({
          taskToken: token,
          input: JSON.stringify(input)
        })
      }),
      sendTaskSuccess: (msg) => ({
        promise: () => {
          t.is(msg.output, JSON.stringify(input));
          t.is(msg.taskToken, token);
          return Promise.resolve();
        }
      })
    });

  await runServiceFromActivity({
    lambdaArn: 'test',
    activityArn: 'test',
    taskDirectory: t.context.taskDirectory,
    workDirectory: t.context.workDirectory,
    runForever: false
  });

  sf.restore();
});

test.serial('test activity failure', async (t) => {
  const input = {
    msg: 'this was a failure',
    error: {
      name: 'failure',
      message: 'it failed'
    }
  };
  const token = 'some token';

  const sf = sinon.stub(AWS, 'StepFunctions')
    .returns({
      getActivityTask: () => ({
        promise: async () => ({
          taskToken: token,
          input: JSON.stringify(input)
        })
      }),
      sendTaskFailure: (msg) => ({
        promise: () => {
          t.is(msg.error, input.error.name);
          t.is(msg.cause, input.error.message);
          return Promise.resolve();
        }
      })
    });

  await runServiceFromActivity({
    lambdaArn: 'test',
    activityArn: 'test',
    taskDirectory: t.context.taskDirectory,
    workDirectory: t.context.workDirectory,
    runForever: false
  });

  sf.restore();
});

test.serial('Retry zip download if connection-timeout received', async (t) => {
  nock.cleanAll();

  const timeoutFailure = nock('https://example.com')
    .get(t.context.lambdaZipUrlPath)
    .replyWithError({ code: 'ETIMEDOUT' });

  nock('https://example.com')
    .get(t.context.lambdaZipUrlPath)
    .reply(200, () => fs.createReadStream(t.context.lambdaZip));

  const event = { hi: 'bye' };

  const output = await runTask({
    lambdaArn: 'arn:aws:lambda:region:account-id:function:fake-function',
    lambdaInput: event,
    taskDirectory: t.context.taskDirectory,
    workDirectory: t.context.workDirectory
  });

  t.true(timeoutFailure.isDone());
  t.deepEqual(event, output);
});
