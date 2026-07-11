const API_BASE = "./v1";
const DELIVERY_STATUS = Object.freeze({
    PENDING: 'pending',
    DELIVERED: 'delivered',
});
const TOAST_TYPE = Object.freeze({
    SUCCESS: 'success',
    ERROR: 'error',
});

// State
let currentTopic = null;
let topics = [];

function escapeHTML(value) {
    return String(value ?? '')
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#039;');
}

function inlineJSON(value) {
    return JSON.stringify(value)
        .replaceAll('&', '\\u0026')
        .replaceAll("'", '\\u0027')
        .replaceAll('<', '\\u003c')
        .replaceAll('>', '\\u003e');
}

function pathSegment(value) {
    return encodeURIComponent(String(value));
}

function pageItems(response, field) {
    const page = response?.data;
    return Array.isArray(page?.[field]) ? page[field] : [];
}

function topicsFromResponse(response) {
    return pageItems(response, 'topics');
}

function subscribersFromResponse(response) {
    return pageItems(response, 'subscribers');
}

function subscriberSummary(subscribers) {
    return subscribers.reduce((summary, subscriber) => ({
        count: summary.count + 1,
        pending: summary.pending + (Number(subscriber.unpublished_message) || 0),
        unacked: summary.unacked + (Number(subscriber.unacked_message) || 0),
    }), { count: 0, pending: 0, unacked: 0 });
}

function topicListHTML(topicItems, activeTopic) {
    return topicItems.map(topic => `
        <div class="topic-item ${activeTopic?.id === topic.id ? 'active' : ''}" onclick='selectTopic(${inlineJSON(topic.name)})'>
            <span><i class="fa-solid fa-hashtag"></i> ${escapeHTML(topic.name)}</span>
            <span style="font-size: 0.75em; opacity: 0.5">${escapeHTML(String(topic.id).substring(0, 8))}</span>
        </div>
    `).join('');
}

function subscriberListHTML(subscribers) {
    if (subscribers.length === 0) {
        return '<div style="text-align:center; padding: 2rem; color: var(--text-secondary)">No subscribers yet.</div>';
    }

    return subscribers.map(subscriber => `
        <div class="subscriber-card">
            <div class="subscriber-info">
                <h3>${escapeHTML(subscriber.name)}</h3>
                <span class="status-badge">Active</span>
            </div>
            <div class="subscriber-stats">
                 <div class="stat-item" title="Pending Messages">
                    <i class="fa-solid fa-hourglass-start" style="color: var(--accent-color)"></i>
                    ${escapeHTML(subscriber.unpublished_message)}
                </div>
                <div class="stat-item" title="Unacked Messages">
                    <i class="fa-solid fa-envelope-open-text" style="color: #fbbf24"></i>
                    ${escapeHTML(subscriber.unacked_message)}
                </div>
                <div class="stat-item">
                     <button class="btn btn-sm btn-primary" onclick='openInspect(${inlineJSON(subscriber.name)})' title="Inspect Queue">
                         <i class="fa-solid fa-eye"></i> Peek
                    </button>
                    <button class="btn btn-sm btn-danger" onclick='openDLQ(${inlineJSON(subscriber.name)})' title="View Dead Letter Queue" style="margin-left: 0.5rem">
                         <i class="fa-solid fa-skull"></i> DLQ
                    </button>
                     <button class="btn btn-sm btn-danger" onclick='deleteSubscriber(${inlineJSON(subscriber.name)})' title="Delete Subscriber" style="margin-left: 0.5rem">
                         <i class="fa-solid fa-trash"></i>
                    </button>
                </div>
            </div>
        </div>
    `).join('');
}

// DOM Elements
const topicListEl = document.getElementById('topicList');
const dashboardView = document.getElementById('dashboardView');
const topicView = document.getElementById('topicView');
const currentTopicNameEl = document.getElementById('currentTopicName');
const topicIdEl = document.getElementById('topicId');
const subscriberListEl = document.getElementById('subscriberList');

// Stats Elements
const statsSubscribers = document.getElementById('statsSubscribers');
const statsPending = document.getElementById('statsPending');
const statsUnacked = document.getElementById('statsUnacked');

// Init
document.addEventListener('DOMContentLoaded', () => {
    fetchTopics();
});

// Navigation & Topics
async function fetchTopics() {
    try {
        const res = await fetch(`${API_BASE}/topics`);
        if (res.ok) {
            const data = await res.json();
            topics = topicsFromResponse(data);
            renderTopics();
        }
    } catch (err) {
        console.error('Failed to fetch topics', err);
    }
}

function renderTopics() {
    topicListEl.innerHTML = topicListHTML(topics, currentTopic);
}

async function selectTopic(name) {
    // We need to fetch subscribers status which gives us topic metadata + subscribers
    try {
        const res = await fetch(`${API_BASE}/topics/${pathSegment(name)}/subscribers`);
        if (!res.ok) throw new Error('Failed to load topic details');

        const response = await res.json();

        const statusData = subscribersFromResponse(response);

        const selectedTopic = topics.find(topic => topic.name === name);
        const topicId = selectedTopic?.id || statusData[0]?.topic_id || 'Unknown';

        currentTopic = { name, id: topicId };
        renderTopics(); // Update active state

        // Update View
        dashboardView.style.display = 'none';
        topicView.style.display = 'block';
        topicView.classList.add('animate-fade-in');

        currentTopicNameEl.textContent = name;
        topicIdEl.textContent = `ID: ${topicId}`;

        // Stats
        const summary = subscriberSummary(statusData);
        statsSubscribers.textContent = summary.count;
        statsPending.textContent = summary.pending;
        statsUnacked.textContent = summary.unacked;

        renderSubscribers(statusData);

    } catch (err) {
        alert(err.message);
    }
}

function renderSubscribers(subscribers) {
    subscriberListEl.innerHTML = subscriberListHTML(subscribers);
}

// Actions
async function handleCreateTopic(e) {
    e.preventDefault();
    const formData = new FormData(e.target);
    const name = formData.get('name');
    const subscriberName = formData.get('subscriber_name');
    const maxAttempts = parseInt(formData.get('max_attempts'));
    const visibilityDuration = formData.get('visibility_duration');

    try {
        const res = await fetch(`${API_BASE}/topics`, {
            method: 'POST',
            body: JSON.stringify({
                name,
                subscribers: [{
                    name: subscriberName,
                    option: {
                        max_attempts: maxAttempts,
                        visibility_duration: visibilityDuration,
                    },
                }],
            }),
            headers: { 'Content-Type': 'application/json' }
        });

        if (!res.ok) throw new Error(await res.text());

        closeModal('createTopicModal');
        e.target.reset();
        fetchTopics(); // Reload list
        selectTopic(name);
        showToast('Topic created!');
    } catch (err) {
        showToast('Error creating topic: ' + err.message, TOAST_TYPE.ERROR);
    }
}

async function deleteTopic() {
    if (!currentTopic || !confirm(`Delete topic ${currentTopic.name}? This cannot be undone.`)) return;

    try {
        const res = await fetch(`${API_BASE}/topics/${pathSegment(currentTopic.name)}/`, { method: 'DELETE' });
        if (!res.ok) throw new Error(await res.text());

        currentTopic = null;
        dashboardView.style.display = 'flex';
        topicView.style.display = 'none';
        fetchTopics();
        showToast('Topic deleted');
    } catch (err) {
        showToast('Error deleting topic: ' + err.message, TOAST_TYPE.ERROR);
    }
}

async function handlePublish(e) {
    e.preventDefault();
    if (!currentTopic) return;

    const btn = e.target.querySelector('button[type="submit"]');
    const originalText = btn.innerHTML;

    // Set loading state
    btn.classList.add('loading');
    btn.innerHTML = ''; // Hide text/icon, spinner shown via CSS

    const formData = new FormData(e.target);
    const message = formData.get('message');
    const delay = formData.get('delay');

    const payload = { message };
    if (delay) payload.delay = delay;

    try {
        const res = await fetch(`${API_BASE}/topics/${pathSegment(currentTopic.name)}/messages?wait_for=commit`, {
            method: 'POST',
            body: JSON.stringify(payload),
            headers: { 'Content-Type': 'application/json' }
        });

        if (!res.ok) throw new Error(await res.text());

        closeModal('publishModal');
        e.target.reset();

        showToast('Message published successfully!', TOAST_TYPE.SUCCESS);

        selectTopic(currentTopic.name);

    } catch (err) {
        showToast('Error publishing: ' + err.message, TOAST_TYPE.ERROR);
    } finally {
        // Reset button
        btn.classList.remove('loading');
        btn.innerHTML = originalText;
    }
}

function showToast(message, type = TOAST_TYPE.SUCCESS) {
    const container = document.getElementById('toastContainer');
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;

    const icon = type === TOAST_TYPE.SUCCESS
        ? '<i class="fa-solid fa-circle-check"></i>'
        : '<i class="fa-solid fa-circle-exclamation"></i>';

    toast.innerHTML = `${icon} <span>${escapeHTML(message)}</span>`;

    container.appendChild(toast);

    // Trigger animation
    requestAnimationFrame(() => {
        toast.classList.add('show');
    });

    // Remove after 3s
    setTimeout(() => {
        toast.classList.remove('show');
        setTimeout(() => toast.remove(), 300);
    }, 3000);
}

async function handleCreateSubscriber(e) {
    e.preventDefault();
    if (!currentTopic) return;

    const formData = new FormData(e.target);
    const name = formData.get('name');
    const option = {
        max_attempts: parseInt(formData.get('max_attempts')),
        visibility_duration: formData.get('visibility_duration')
    };

    try {
        const res = await fetch(`${API_BASE}/topics/${pathSegment(currentTopic.name)}/subscribers`, {
            method: 'POST',
            body: JSON.stringify([{
                name,
                option
            }]),
            headers: { 'Content-Type': 'application/json' }
        });

        if (!res.ok) throw new Error(await res.text());

        closeModal('createSubscriberModal');
        e.target.reset();
        selectTopic(currentTopic.name);
        showToast('Subscriber created');
    } catch (err) {
        showToast('Error creating subscriber: ' + err.message, TOAST_TYPE.ERROR);
    }
}

async function deleteSubscriber(name) {
    if (!currentTopic || !confirm(`Delete subscriber ${name}?`)) return;

    try {
        const res = await fetch(`${API_BASE}/topics/${pathSegment(currentTopic.name)}/subscribers/${pathSegment(name)}`, { method: 'DELETE' });
        if (!res.ok) throw new Error(await res.text());
        selectTopic(currentTopic.name);
        showToast('Subscriber deleted');
    } catch (err) {
        showToast('Error deleting subscriber: ' + err.message, TOAST_TYPE.ERROR);
    }
}

async function openDLQ(subscriberName) {
    const modal = document.getElementById('dlqModal');
    const content = document.getElementById('dlqContent');
    content.innerHTML = '<div style="text-align:center">Loading...</div>';
    openModal('dlqModal');

    try {
        const res = await fetch(`${API_BASE}/topics/${pathSegment(currentTopic.name)}/subscribers/${pathSegment(subscriberName)}/dlq`);
        if (!res.ok) throw new Error('Failed to fetch DLQ');

        const response = await res.json();
        const messages = response.data?.messages || [];

        if (messages.length === 0) {
            content.innerHTML = '<div style="text-align:center; padding: 2rem;">No messages in DLQ</div>';
            return;
        }

        content.innerHTML = messages.map(m => `
            <div class="subscriber-card" style="margin-bottom: 0.5rem">
                <div class="subscriber-info">
                    <div style="font-family:monospace; font-size:0.8rem; opacity:0.7">${escapeHTML(m.id)}</div>
                    <div style="margin-top:0.25rem">${escapeHTML(m.message)}</div>
                </div>
                <div class="subscriber-stats">
                    <button class="btn btn-sm btn-primary" onclick='replayDLQ(${inlineJSON(subscriberName)}, ${inlineJSON(m.id)})'>
                        <i class="fa-solid fa-rotate-left"></i> Replay
                    </button>
                </div>
            </div>
        `).join('');

    } catch (err) {
        content.innerHTML = `<div style="color:var(--error-color)">Error: ${escapeHTML(err.message)}</div>`;
    }
}

async function openInspect(subscriberName) {
    const modal = document.getElementById('inspectModal');
    const content = document.getElementById('inspectContent');
    content.innerHTML = '<div style="text-align:center">Loading...</div>';
    openModal('inspectModal');

    try {
        const res = await fetch(`${API_BASE}/topics/${pathSegment(currentTopic.name)}/subscribers/${pathSegment(subscriberName)}/messages`);
        if (!res.ok) throw new Error('Failed to fetch messages');

        const response = await res.json();
        const messages = response.data?.messages || [];

        if (messages.length === 0) {
            content.innerHTML = '<div style="text-align:center; padding: 2rem;">Queue is empty (no pending/delivered messages)</div>';
            return;
        }

        content.innerHTML = messages.map(m => `
            <div class="subscriber-card" style="margin-bottom: 0.5rem; flex-direction: column; align-items: flex-start;">
                <div style="width: 100%; display: flex; justify-content: space-between; align-items: start; margin-bottom: 0.5rem;">
                     <div>
                        <div style="font-family:monospace; font-size:0.75rem; opacity:0.6">${escapeHTML(m.id)}</div>
                        <div style="font-size: 0.75rem; color: var(--text-secondary); margin-top: 0.1rem;">
                            <i class="fa-regular fa-clock"></i> ${new Date(m.created_at).toLocaleString()}
                        </div>
                    </div>
                    <div style="display: flex; gap: 0.5rem; align-items: center;">
                        <span class="status-badge ${m.status === DELIVERY_STATUS.PENDING ? 'warning' : 'success'}">${m.status}</span>
                         ${m.status === DELIVERY_STATUS.DELIVERED ? `<button class="btn btn-sm btn-primary" onclick='ackMessage(${inlineJSON(subscriberName)}, ${inlineJSON(m.id)}, ${inlineJSON(m.receipt_token)})' title="Ack Message" style="padding: 0.2rem 0.6rem; font-size: 0.7rem;">
                            <i class="fa-solid fa-check"></i> Ack
                        </button>` : ''}
                    </div>
                </div>
                <div style="background: rgba(0,0,0,0.2); padding: 0.75rem; border-radius: 0.5rem; width: 100%; font-family: monospace; white-space: pre-wrap; font-size: 0.85rem;">${escapeHTML(m.message)}</div>
                ${m.status === DELIVERY_STATUS.DELIVERED ? `
                    <div style="margin-top: 0.5rem; font-size: 0.75rem; color: var(--accent-color);">
                        <i class="fa-solid fa-stopwatch"></i> Lease expires at: ${new Date(m.lease_expires_at).toLocaleTimeString()}
                    </div>
                ` : ''}
            </div>
        `).join('');

    } catch (err) {
        content.innerHTML = `<div style="color:var(--error-color)">Error: ${escapeHTML(err.message)}</div>`;
    }
}

async function ackMessage(subscriberName, messageID, receiptToken) {
    if (!confirm('Acknowledge this delivery?')) return;

    try {
        const res = await fetch(`${API_BASE}/topics/${pathSegment(currentTopic.name)}/subscribers/${pathSegment(subscriberName)}/messages/${pathSegment(messageID)}/ack`, {
            method: 'POST',
            body: JSON.stringify({ receipt_token: receiptToken }),
            headers: { 'Content-Type': 'application/json' },
        });
        if (!res.ok) throw new Error('Failed to ack message');

        showToast('Message acknowledged');
        // Refresh list
        openInspect(subscriberName);
        // Refresh stats
        selectTopic(currentTopic.name);
    } catch (err) {
        showToast('Error acking message: ' + err.message, TOAST_TYPE.ERROR);
    }
}

async function replayDLQ(subscriberName, messageID) {
    try {
        const res = await fetch(`${API_BASE}/topics/${pathSegment(currentTopic.name)}/subscribers/${pathSegment(subscriberName)}/dlq/replay`, {
            method: 'POST',
            body: JSON.stringify({ message_ids: [messageID] }),
            headers: { 'Content-Type': 'application/json' },
        });
        if (!res.ok) throw new Error('Failed to replay');

        // Refresh DLQ list
        openDLQ(subscriberName);
        // Refresh stats
        selectTopic(currentTopic.name);
        showToast('Message replayed');
    } catch (err) {
        showToast('Error replaying message: ' + err.message, TOAST_TYPE.ERROR);
    }
}

// Modal Utils
window.openModal = (id) => {
    document.getElementById(id).classList.add('open');
}

window.closeModal = (id) => {
    document.getElementById(id).classList.remove('open');
}

window.onclick = (e) => {
    if (e.target.classList.contains('modal-overlay')) {
        e.target.classList.remove('open');
    }
}

// Make functions global for HTML inline handlers
window.handleCreateTopic = handleCreateTopic;
window.handlePublish = handlePublish;
window.handleCreateSubscriber = handleCreateSubscriber;
window.deleteTopic = deleteTopic;
window.deleteSubscriber = deleteSubscriber;
window.openDLQ = openDLQ;
window.openInspect = openInspect;
window.replayDLQ = replayDLQ;
window.ackMessage = ackMessage;
selectTopic = selectTopic; // Make sure this is accessible for topic list click

if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        subscriberListHTML,
        subscriberSummary,
        subscribersFromResponse,
        topicListHTML,
        topicsFromResponse,
    };
}
