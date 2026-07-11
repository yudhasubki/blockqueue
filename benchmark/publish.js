import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://127.0.0.1:8090';
const API_URL = `${BASE_URL}${__ENV.API_PREFIX || '/v1'}`;
const TOPIC = __ENV.TOPIC || 'cart';
const TOPIC_PATH = encodeURIComponent(TOPIC);
const SUBSCRIBER = __ENV.SUBSCRIBER || 'counter';
const RATE = Number(__ENV.RATE || 0);
const DURATION = __ENV.DURATION || '15s';
const PRE_ALLOCATED_VUS = Number(__ENV.PRE_ALLOCATED_VUS || 50);
const MAX_VUS = Number(__ENV.MAX_VUS || 200);
const DEQUEUE_BATCH_SIZE = Number(__ENV.DEQUEUE_BATCH_SIZE || 100);
const EXECUTOR = __ENV.EXECUTOR || (RATE > 0 ? 'constant-arrival-rate' : '');
const VUS = Number(__ENV.VUS || 100);
const PUBLISH_BATCH_SIZE = Number(__ENV.PUBLISH_BATCH_SIZE || 1);
const WAIT_FOR_COMMIT = ['1', 'true', 'yes'].includes(
  (__ENV.WAIT_FOR_COMMIT || '').toLowerCase(),
);
const publishedMessages = new Counter('published_messages');

const publishScenario = EXECUTOR === 'constant-vus'
  ? {
      executor: 'constant-vus',
      vus: VUS,
      duration: DURATION,
      gracefulStop: '5s',
    }
  : EXECUTOR === 'constant-arrival-rate'
    ? {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: PRE_ALLOCATED_VUS,
      maxVUs: MAX_VUS,
      gracefulStop: '5s',
    }
    : null;

export const options = publishScenario
  ? {
      discardResponseBodies: true,
      scenarios: { publish: publishScenario },
      thresholds: { 'checks{scenario:publish}': ['rate>0.99'] },
    }
  : {
      discardResponseBodies: true,
      thresholds: { checks: ['rate>0.99'] },
    };

const jsonParams = {
  headers: { 'Content-Type': 'application/json' },
};

const setupParams = {
  headers: { 'Content-Type': 'application/json' },
  responseCallback: http.expectedStatuses({ min: 200, max: 299 }, 409),
};

export function setup() {
  const response = http.post(
    `${API_URL}/topics`,
    JSON.stringify({
      name: TOPIC,
      subscribers: [
        {
          name: SUBSCRIBER,
          option: {
            max_attempts: 5,
            visibility_duration: '30s',
            dequeue_batch_size: DEQUEUE_BATCH_SIZE,
          },
        },
      ],
    }),
    setupParams,
  );

  check(response, {
    'topic is ready': (res) => (res.status >= 200 && res.status < 300) || res.status === 409,
  });
}

export default function () {
  const batched = PUBLISH_BATCH_SIZE > 1;
  const payload = batched
    ? Array.from({ length: PUBLISH_BATCH_SIZE }, (_, index) => ({
        message: `k6-${__VU}-${__ITER}-${index}`,
      }))
    : {
        message: `k6-${__VU}-${__ITER}`,
      };
  const response = http.post(
    `${API_URL}/topics/${TOPIC_PATH}/messages${batched ? '/batch' : ''}${WAIT_FOR_COMMIT ? '?wait_for=commit' : ''}`,
    JSON.stringify(payload),
    jsonParams,
  );

  const published = check(response, {
    'publish status is 2xx': (res) => res.status >= 200 && res.status < 300,
  });
  if (published) {
    publishedMessages.add(PUBLISH_BATCH_SIZE);
  }
}
