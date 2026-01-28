const API_BASE = window.location.origin;

// State
let currentTopic = null;
let topics = [];

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
            // Adjust based on actual API response structure
            topics = data.data || [];
            renderTopics();
        }
    } catch (err) {
        console.error('Failed to fetch topics', err);
    }
}

function renderTopics() {
    topicListEl.innerHTML = topics.map(t => `
        <div class="topic-item ${currentTopic?.id === t.id ? 'active' : ''}" onclick="selectTopic('${t.name}')">
            <span><i class="fa-solid fa-hashtag"></i> ${t.name}</span>
            <span style="font-size: 0.75em; opacity: 0.5">${t.id.substring(0, 8)}</span>
        </div>
    `).join('');
}

async function selectTopic(name) {
    // We need to fetch subscribers status which gives us topic metadata + subscribers
    try {
        const res = await fetch(`${API_BASE}/topics/${name}/subscribers`);
        if (!res.ok) throw new Error('Failed to load topic details');

        const response = await res.json();

        const statusData = response.data || [];

        const topicId = statusData.length > 0 ? statusData[0].topic_id : 'Unknown'; /* We need to fix this in backend if topic has no subscribers */

        currentTopic = { name, id: topicId };
        renderTopics(); // Update active state

        // Update View
        dashboardView.style.display = 'none';
        topicView.style.display = 'block';
        topicView.classList.add('animate-fade-in');

        currentTopicNameEl.textContent = name;
        topicIdEl.textContent = `ID: ${topicId}`;

        // Stats
        statsSubscribers.textContent = statusData.length;
        const totalPending = statusData.reduce((acc, s) => acc + s.unpublished_message, 0);
        const totalUnacked = statusData.reduce((acc, s) => acc + s.unacked_message, 0);

        statsPending.textContent = totalPending;
        statsUnacked.textContent = totalUnacked;

        renderSubscribers(statusData);

    } catch (err) {
        alert(err.message);
    }
}

function renderSubscribers(subscribers) {
    if (subscribers.length === 0) {
        subscriberListEl.innerHTML = '<div style="text-align:center; padding: 2rem; color: var(--text-secondary)">No subscribers yet.</div>';
        return;
    }

    subscriberListEl.innerHTML = subscribers.map(s => `
        <div class="subscriber-card">
            <div class="subscriber-info">
                <h3>${s.name}</h3>
                <span class="status-badge">Active</span>
            </div>
            <div class="subscriber-stats">
                 <div class="stat-item" title="Pending Messages">
                    <i class="fa-solid fa-hourglass-start" style="color: var(--accent-color)"></i>
                    ${s.unpublished_message}
                </div>
                <div class="stat-item" title="Unacked Messages">
                    <i class="fa-solid fa-envelope-open-text" style="color: #fbbf24"></i>
                    ${s.unacked_message}
                </div>
                <div class="stat-item">
                     <button class="btn btn-sm btn-primary" onclick="openInspect('${s.name}')" title="Inspect Queue">
                         <i class="fa-solid fa-eye"></i> Peek
                    </button>
                    <button class="btn btn-sm btn-danger" onclick="openDLQ('${s.name}')" title="View Dead Letter Queue" style="margin-left: 0.5rem">
                         <i class="fa-solid fa-skull"></i> DLQ
                    </button>
                     <button class="btn btn-sm btn-danger" onclick="deleteSubscriber('${s.name}')" title="Delete Subscriber" style="margin-left: 0.5rem">
                         <i class="fa-solid fa-trash"></i>
                    </button>
                </div>
            </div>
        </div>
    `).join('');
}

// Actions
async function handleCreateTopic(e) {
    e.preventDefault();
    const formData = new FormData(e.target);
    const name = formData.get('name');

    try {
        const res = await fetch(`${API_BASE}/topics`, {
            method: 'POST',
            body: JSON.stringify({ name }),
            headers: { 'Content-Type': 'application/json' }
        });

        if (!res.ok) throw new Error(await res.text());

        closeModal('createTopicModal');
        e.target.reset();
        fetchTopics(); // Reload list
        selectTopic(name);
        showToast('Topic created!');
    } catch (err) {
        showToast('Error creating topic: ' + err.message, 'error');
    }
}

async function deleteTopic() {
    if (!currentTopic || !confirm(`Delete topic ${currentTopic.name}? This cannot be undone.`)) return;

    try {
        const res = await fetch(`${API_BASE}/topics/${currentTopic.name}`, { method: 'DELETE' });
        if (!res.ok) throw new Error(await res.text());

        currentTopic = null;
        dashboardView.style.display = 'flex';
        topicView.style.display = 'none';
        fetchTopics();
        showToast('Topic deleted');
    } catch (err) {
        showToast('Error deleting topic: ' + err.message, 'error');
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
        const res = await fetch(`${API_BASE}/topics/${currentTopic.name}/messages`, {
            method: 'POST',
            body: JSON.stringify(payload),
            headers: { 'Content-Type': 'application/json' }
        });

        if (!res.ok) throw new Error(await res.text());

        closeModal('publishModal');
        e.target.reset();

        showToast('Message published successfully!', 'success');

        // Small delay to ensure DB write is visible in stats
        setTimeout(() => {
            selectTopic(currentTopic.name);
        }, 300);

    } catch (err) {
        showToast('Error publishing: ' + err.message, 'error');
    } finally {
        // Reset button
        btn.classList.remove('loading');
        btn.innerHTML = originalText;
    }
}

function showToast(message, type = 'success') {
    const container = document.getElementById('toastContainer');
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;

    const icon = type === 'success'
        ? '<i class="fa-solid fa-circle-check"></i>'
        : '<i class="fa-solid fa-circle-exclamation"></i>';

    toast.innerHTML = `${icon} <span>${message}</span>`;

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
        const res = await fetch(`${API_BASE}/topics/${currentTopic.name}/subscribers`, {
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
        showToast('Error creating subscriber: ' + err.message, 'error');
    }
}

async function deleteSubscriber(name) {
    if (!currentTopic || !confirm(`Delete subscriber ${name}?`)) return;

    try {
        const res = await fetch(`${API_BASE}/topics/${currentTopic.name}/subscribers/${name}`, { method: 'DELETE' });
        if (!res.ok) throw new Error(await res.text());
        selectTopic(currentTopic.name);
        showToast('Subscriber deleted');
    } catch (err) {
        showToast('Error deleting subscriber: ' + err.message, 'error');
    }
}

async function openDLQ(subscriberName) {
    const modal = document.getElementById('dlqModal');
    const content = document.getElementById('dlqContent');
    content.innerHTML = '<div style="text-align:center">Loading...</div>';
    openModal('dlqModal');

    try {
        const res = await fetch(`${API_BASE}/topics/${currentTopic.name}/subscribers/${subscriberName}/dlq`);
        if (!res.ok) throw new Error('Failed to fetch DLQ');

        const response = await res.json();
        const messages = response.data || [];

        if (messages.length === 0) {
            content.innerHTML = '<div style="text-align:center; padding: 2rem;">No messages in DLQ</div>';
            return;
        }

        content.innerHTML = messages.map(m => `
            <div class="subscriber-card" style="margin-bottom: 0.5rem">
                <div class="subscriber-info">
                    <div style="font-family:monospace; font-size:0.8rem; opacity:0.7">${m.id}</div>
                    <div style="margin-top:0.25rem">${m.message}</div>
                </div>
                <div class="subscriber-stats">
                    <button class="btn btn-sm btn-primary" onclick="replayDLQ('${subscriberName}', '${m.id}')">
                        <i class="fa-solid fa-rotate-left"></i> Replay
                    </button>
                </div>
            </div>
        `).join('');

    } catch (err) {
        content.innerHTML = `<div style="color:var(--error-color)">Error: ${err.message}</div>`;
    }
}

async function openInspect(subscriberName) {
    const modal = document.getElementById('inspectModal');
    const content = document.getElementById('inspectContent');
    content.innerHTML = '<div style="text-align:center">Loading...</div>';
    openModal('inspectModal');

    try {
        const res = await fetch(`${API_BASE}/topics/${currentTopic.name}/subscribers/${subscriberName}/messages`);
        if (!res.ok) throw new Error('Failed to fetch messages');

        const response = await res.json();
        const messages = response.data || [];

        if (messages.length === 0) {
            content.innerHTML = '<div style="text-align:center; padding: 2rem;">Queue is empty (no pending/delivered messages)</div>';
            return;
        }

        content.innerHTML = messages.map(m => `
            <div class="subscriber-card" style="margin-bottom: 0.5rem; flex-direction: column; align-items: flex-start;">
                <div style="width: 100%; display: flex; justify-content: space-between; align-items: start; margin-bottom: 0.5rem;">
                     <div>
                        <div style="font-family:monospace; font-size:0.75rem; opacity:0.6">${m.id}</div>
                        <div style="font-size: 0.75rem; color: var(--text-secondary); margin-top: 0.1rem;">
                            <i class="fa-regular fa-clock"></i> ${new Date(m.created_at).toLocaleString()}
                        </div>
                    </div>
                    <div style="display: flex; gap: 0.5rem; align-items: center;">
                        <span class="status-badge ${m.status === 'pending' ? 'warning' : 'success'}">${m.status}</span>
                         <button class="btn btn-sm btn-primary" onclick="ackMessage('${subscriberName}', '${m.id}')" title="Ack Message" style="padding: 0.2rem 0.6rem; font-size: 0.7rem;">
                            <i class="fa-solid fa-check"></i> Ack
                        </button>
                    </div>
                </div>
                <div style="background: rgba(0,0,0,0.2); padding: 0.75rem; border-radius: 0.5rem; width: 100%; font-family: monospace; white-space: pre-wrap; font-size: 0.85rem;">${m.message}</div>
                ${m.status === 'delivered' ? `
                    <div style="margin-top: 0.5rem; font-size: 0.75rem; color: var(--accent-color);">
                        <i class="fa-solid fa-stopwatch"></i> Visible again at: ${new Date(m.visible_at).toLocaleTimeString()}
                    </div>
                ` : ''}
            </div>
        `).join('');

    } catch (err) {
        content.innerHTML = `<div style="color:var(--error-color)">Error: ${err.message}</div>`;
    }
}

async function ackMessage(subscriberName, messageId) {
    if (!confirm('Are you sure you want to ACK (delete) this message?')) return;

    try {
        const res = await fetch(`${API_BASE}/topics/${currentTopic.name}/subscribers/${subscriberName}/messages/${messageId}`, {
            method: 'DELETE'
        });
        if (!res.ok) throw new Error('Failed to ack message');

        showToast('Message acknowledged');
        // Refresh list
        openInspect(subscriberName);
        // Refresh stats
        selectTopic(currentTopic.name);
    } catch (err) {
        showToast('Error acking message: ' + err.message, 'error');
    }
}

async function replayDLQ(subscriberName, messageId) {
    try {
        const res = await fetch(`${API_BASE}/topics/${currentTopic.name}/subscribers/${subscriberName}/dlq/${messageId}/replay`, {
            method: 'POST'
        });
        if (!res.ok) throw new Error('Failed to replay');

        // Refresh DLQ list
        openDLQ(subscriberName);
        // Refresh stats
        selectTopic(currentTopic.name);
        showToast('Message replayed');
    } catch (err) {
        showToast('Error replaying message: ' + err.message, 'error');
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
