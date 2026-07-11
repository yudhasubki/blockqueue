'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const elements = new Map();
global.document = {
    addEventListener() {},
    createElement() {
        return { classList: { add() {}, remove() {} }, remove() {} };
    },
    getElementById(id) {
        if (!elements.has(id)) {
            elements.set(id, {
                classList: { add() {}, remove() {} },
                innerHTML: '',
                style: {},
                textContent: '',
            });
        }
        return elements.get(id);
    },
};
global.window = {};

const dashboard = require(path.join(__dirname, '..', '..', 'ui', 'app.js'));
const fixture = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));

const topics = dashboard.topicsFromResponse(fixture.topics);
assert.equal(topics.length, 2);
const orders = topics.find(topic => topic.name === 'orders');
assert.ok(orders);
const topicHTML = dashboard.topicListHTML(topics, orders);
assert.match(topicHTML, /topic-item active/);
assert.match(topicHTML, /returns&lt;script&gt;/);
assert.doesNotMatch(topicHTML, /returns<script>/);

const subscribers = dashboard.subscribersFromResponse(fixture.subscribers);
assert.equal(subscribers.length, 2);
assert.deepEqual(dashboard.subscriberSummary(subscribers), {
    count: 2,
    pending: 1,
    unacked: 1,
});
const subscriberHTML = dashboard.subscriberListHTML(subscribers);
assert.match(subscriberHTML, /worker&lt;primary&gt;/);
assert.match(subscriberHTML, />\s*1\s*</);
assert.doesNotMatch(subscriberHTML, /worker<primary>/);
