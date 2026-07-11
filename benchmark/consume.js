import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://127.0.0.1:8090';
const API_URL = `${BASE_URL}${__ENV.API_PREFIX || '/v1'}`;
const TOPIC = __ENV.TOPIC || 'cart';
const SUBSCRIBER = __ENV.SUBSCRIBER || 'counter';
const TOPIC_PATH = encodeURIComponent(TOPIC);
const SUBSCRIBER_PATH = encodeURIComponent(SUBSCRIBER);
const VUS = Number(__ENV.VUS || 20);
const DURATION = __ENV.DURATION || '15s';
const LONG_POLL_TIMEOUT = __ENV.LONG_POLL_TIMEOUT || '1s';

const consumedMessages = new Counter('consumed_messages');
const ackFailures = new Counter('ack_failures');

export const options = {
  scenarios: {
    consume: {
      executor: 'constant-vus',
      vus: VUS,
      duration: DURATION,
      gracefulStop: '5s',
    },
  },
  thresholds: {
    'checks{scenario:consume}': ['rate>0.99'],
    ack_failures: ['count==0'],
  },
};

const jsonParams = {
  headers: { 'Content-Type': 'application/json' },
};

export default function () {
  const response = http.post(
    `${API_URL}/topics/${TOPIC_PATH}/subscribers/${SUBSCRIBER_PATH}/claim?timeout=${LONG_POLL_TIMEOUT}&limit=100`,
    null,
  );

  const readOK = check(response, {
    'consume status is 2xx': (res) => res.status >= 200 && res.status < 300,
  });
  if (!readOK) {
    return;
  }

  let body;
  try {
    body = response.json();
  } catch (_) {
    check(response, { 'consume response is JSON': () => false });
    return;
  }

  const messages = Array.isArray(body.data) ? body.data : [];
  if (messages.length === 0) {
    return;
  }

  consumedMessages.add(messages.length);
  const receipts = messages.map((message) => ({
    message_id: message.id,
    receipt_token: message.receipt_token,
  }));
  const ackResponse = http.post(
    `${API_URL}/topics/${TOPIC_PATH}/subscribers/${SUBSCRIBER_PATH}/messages/batch/ack`,
    JSON.stringify(receipts),
    jsonParams,
  );

  let ackResults = [];
  try {
    ackResults = ackResponse.json('data') || [];
  } catch (_) {
    // The check below records a malformed response as a benchmark failure.
  }
  const ackOK = check(ackResponse, {
    'all deliveries are acknowledged': (res) =>
      res.status >= 200 && res.status < 300 &&
      ackResults.length === receipts.length &&
      ackResults.every((result) => result.status === 'processed'),
  });
  if (!ackOK) {
    ackFailures.add(1);
  }
}
