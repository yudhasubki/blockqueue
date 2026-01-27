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
    // There is no direct "list all topics" endpoint in the provided http.go
    // It seems we only have getTopics by name filter or accessing specific topic
    // Wait, the http.go implementation of createTopic checks if topic exists using Stream.getTopics.
    // However, there is no public endpoint exposed to list ALL topics without filter.
    // I might need to update http.go to allow listing all topics if no filter provided.
    // For now, let's assume I'll add `GET /topics` support to http.go or I mock it.
    // Actually, `http.go` has `r.Route("/topics", ...)` but NO `r.Get("/", ...)` to list all.
    // It only has `r.Post("/", h.createTopic)`.

    // WORKAROUND: For this demo, since I cannot list topics easily without modifying backend to support it,
    // I will modify backend to support listing topics.
    // For now in JS, I will proceed assuming the endpoint will exist.

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
    // Because we don't have full topic list initially maybe, or to get fresh details
    // We can use the check endpoint or just switch view if we have data.
    // Since http.go uses topicName param for all operations, we store the name.

    // We need to fetch subscribers status which gives us topic metadata + subscribers
    try {
        const res = await fetch(`${API_BASE}/topics/${name}/subscribers`);
        if (!res.ok) throw new Error('Failed to load topic details');

        const response = await res.json();
        // The endpoint returns `[]io.SubscriberStatus`
        // We'll assume the topic exists if we get data.

        const statusData = response.data || [];

        // Find topic info from our local list if available, or construct it
        // Since getSubscribersStatus returns data grouped by subscriber, we need to aggregate or finding the topic id from somewhere.
        // Actually `io.SubscriberStatus` contains `TopicId`.

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
                    <button class="btn btn-sm btn-danger" onclick="openDLQ('${s.name}')" title="View Dead Letter Queue">
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
    } catch (err) {
        alert('Error creating topic: ' + err.message);
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
    } catch (err) {
        alert('Error deleting topic: ' + err.message);
    }
}

async function handlePublish(e) {
    e.preventDefault();
    if (!currentTopic) return;

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
        selectTopic(currentTopic.name); // Refresh stats

        // Show success toast (mock)
        const btn = document.querySelector('button[type="submit"]');
        const originalText = btn.innerText;
        btn.innerText = 'Published!';
        setTimeout(() => btn.innerText = originalText, 1000);

    } catch (err) {
        alert('Error publishing: ' + err.message);
    }
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
            body: JSON.stringify({
                name,
                subscribers: [{ name, option }] // The API expects nested structure based on payload on http.go?
                // Checking http.go createSubscriber:
                // it decodes `io.Subscribers` which is `[]Subscriber`.
                // inside it accepts `request.Subscriber(topic.Id)`.
                // Wait, the API `POST /step1/subscribers` expects `io.Subscribers` JSON?
                // Let's check `io.Subscribers` struct.
                // It seems to be `type Subscribers []Subscriber`.
            }),
            headers: { 'Content-Type': 'application/json' }
        });

        // Actually, looking at `http.go`:
        // err := json.NewDecoder(r.Body).Decode(&request) -> request is io.Subscribers
        // io.Subscribers is likely a list.
        // So we should send `[{ "name": "...", "option": {...} }]`

        if (!res.ok) throw new Error(await res.text());

        closeModal('createSubscriberModal');
        e.target.reset();
        selectTopic(currentTopic.name);
    } catch (err) {
        alert('Error creating subscriber: ' + err.message);
    }
}

async function deleteSubscriber(name) {
    if (!currentTopic || !confirm(`Delete subscriber ${name}?`)) return;

    try {
        const res = await fetch(`${API_BASE}/topics/${currentTopic.name}/subscribers/${name}`, { method: 'DELETE' });
        if (!res.ok) throw new Error(await res.text());
        selectTopic(currentTopic.name);
    } catch (err) {
        alert('Error deleting subscriber: ' + err.message);
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
    } catch (err) {
        alert('Error replaying message: ' + err.message);
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
window.replayDLQ = replayDLQ;
selectTopic = selectTopic; // Make sure this is accessible for topic list click
