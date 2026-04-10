const API_URL = window.location.origin;
console.log("🚀 Dashboard Script Loaded!");

// Auth State
let userRole = null;
let userToken = localStorage.getItem('dash_token');
let analyticsChart = null;
let consoleInterval = null;

// Global Fetch Wrapper with Auth
async function authFetch(url, options = {}) {
    if (!options.headers) options.headers = {};
    if (userToken) {
        options.headers['Authorization'] = `Bearer ${userToken}`;
    }
    
    const response = await fetch(url, options);
    
    if (response.status === 401 && !url.includes('/api/auth/login')) {
        // Token expired or invalid
        localStorage.removeItem('dash_token');
        window.location.reload();
    }
    
    return response;
}

let currentCategory = 'all';

// Tab Switching
function showTab(tabId, element, category = 'all') {
    document.querySelectorAll('.tab-content').forEach(tab => tab.classList.remove('active'));
    document.querySelectorAll('.sidebar-item').forEach(item => item.classList.remove('active'));
    
    const targetTab = document.getElementById(`tab-${tabId}`);
    if (targetTab) {
        targetTab.classList.add('active');
        if (element) element.classList.add('active');
    }

    // Tab-specific loading
    if (tabId === 'dashboard') {
        loadStats();
        updateAnalyticsChart();
        startConsoleStream();
    } else {
        stopConsoleStream();
    }
    if (tabId === 'commands') {
        currentCategory = category;
        const titleMap = {
            'all': 'All Commands',
            'Moderation': '🛡️ Moderation Commands',
            'Utility': '🛠️ Utility Commands',
            'Economy': '💰 Economy Commands'
        };
        const headerTitle = document.getElementById('commands-title');
        if (headerTitle) headerTitle.textContent = titleMap[category] || 'Commands Management';
        
        loadCommands();
    }
    if (tabId === 'logs') loadLogs();
    if (tabId === 'members') loadMembers();
    if (tabId === 'ticket-logs') loadTicketLogs();
    if (tabId === 'settings') loadSettings();
    if (tabId === 'ekip-logs') loadSpecificLogs('ekip-log', 'ekip-logs-body');
    if (tabId === 'yayinci-logs') loadSpecificLogs('yayinci-log', 'yayinci-logs-body');
    if (tabId === 'uyari-logs') loadSpecificLogs('moderation-log', 'uyari-logs-body');
    
    if (tabId === 'rules') loadRules();
    if (tabId === 'suggestions') loadSuggestions();
    if (tabId === 'apply') loadApplications();
    if (tabId === 'tickets') loadTicketConfig();
    if (tabId === 'ekip') loadEkipConfig();
    if (tabId === 'yayinci') loadYayinciConfig();
    if (tabId === 'uyari') loadUyariConfig();
    if (tabId === 'access-control') loadTokens();
    if (tabId === 'auto-responder') loadAutoResponders();
    if (tabId === 'embed-builder') {
        loadEmbedBuilder();
    }
    if (tabId === 'automod') loadAutoMod();
    if (tabId === 'invite-logs') {
        loadInviteLeaderboard();
        loadSpecificLogs('invite-log', 'invite-logs-body');
    }
    if (tabId === 'apply-logs') loadApplications();
    if (tabId === 'giveaway') loadActiveGiveaways();
    if (tabId === 'voice-logs') loadSpecificLogs('voice-log', 'voice-logs-body');
}

// Authentication Logic
async function handleLogin() {
    const tokenInput = document.getElementById('login-token');
    const errorMsg = document.getElementById('login-error');
    const token = tokenInput.value.trim();

    if (!token) return;

    try {
        const response = await fetch(`${API_URL}/api/auth/login`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ token })
        });

        if (response.ok) {
            const data = await response.json();
            localStorage.setItem('dash_token', data.token);
            userToken = data.token;
            userRole = data.role;
            document.getElementById('auth-overlay').style.display = 'none';
            setupRoleUI();
            loadStats();
        } else {
            const error = await response.json();
            errorMsg.textContent = error.detail || "Geçersiz token!";
            errorMsg.style.display = 'block';
        }
    } catch (e) {
        console.error("Login Error:", e);
        errorMsg.textContent = "Bağlantı hatası oluştu!";
        errorMsg.style.display = 'block';
    }
}

async function checkAuth() {
    if (!userToken) {
        document.getElementById('auth-overlay').style.display = 'flex';
        return;
    }

    try {
        const response = await authFetch(`${API_URL}/api/auth/me`);
        if (response.ok) {
            const data = await response.json();
            userRole = data.role;
            document.getElementById('auth-overlay').style.display = 'none';
            setupRoleUI();
            loadStats();
        } else {
            document.getElementById('auth-overlay').style.display = 'flex';
        }
    } catch (e) {
        document.getElementById('auth-overlay').style.display = 'flex';
    }
}

function setupRoleUI() {
    const isOwner = userRole === 'owner';
    const isAdmin = userRole === 'admin';
    const isStaff = userRole === 'staff';
    const isTrial = userRole === 'trial_staff';

    // Access Control sadece Owner için
    document.getElementById('nav-access-control').style.display = isOwner ? 'flex' : 'none';

    // Tüm kısıtlanabilir elementlerin listesi
    const allElements = [
        'nav-settings', 'nav-docs',
        'label-commands', 'nav-commands-all', 'nav-commands-mod', 'nav-commands-util', 'nav-commands-econ',
        'label-config', 'nav-logs-general', 'nav-rules', 'nav-suggestions', 'nav-apply', 'nav-auto-responder',
        'label-logs-cat', 'nav-ticket-logs', 'nav-ekip-logs', 'nav-yayinci-logs', 'nav-uyari-logs', 'nav-invite-logs', 'nav-apply-logs',
        'label-bots', 'nav-tickets', 'nav-ekip', 'nav-yayinci', 'nav-uyari', 'nav-embed-builder', 'nav-giveaway'
    ];

    allElements.forEach(id => {
        const el = document.getElementById(id);
        if (!el) return;

        if (isOwner) {
            el.style.display = 'flex';
        } else if (isTrial) {
            // Trial Staff sadece Dashboard (her zaman açık), Uyarı Log ve Logs görsün
            const trialAllowed = ['nav-dashboard', 'nav-uyari-logs', 'nav-logs-general', 'label-config', 'label-logs-cat'];
            el.style.display = trialAllowed.includes(id) ? 'flex' : 'none';
        } else if (isAdmin || isStaff) {
            // Admin ve Staff için belirtilen kısıtlamalar
            const restrictedForStaff = [
                'nav-settings', 
                'label-commands', 'nav-commands-all', 'nav-commands-mod', 'nav-commands-util', 'nav-commands-econ',
                'label-bots', 'nav-tickets', 'nav-ekip', 'nav-yayinci', 'nav-uyari'
            ];
            el.style.display = restrictedForStaff.includes(id) ? 'none' : 'flex';
        }
    });

    // Dashboard her zaman görünür (ID: nav-dashboard)
    const dash = document.getElementById('nav-dashboard');
    if (dash) dash.style.display = 'flex';
}

// Token Management Functions
async function loadTokens() {
    if (userRole !== 'owner') return;
    
    try {
        const response = await authFetch(`${API_URL}/api/tokens`);
        const tokens = await response.json();
        const body = document.getElementById('token-list-body');
        body.innerHTML = '';

        tokens.forEach(t => {
            const isSelf = t.token === userToken;
            body.innerHTML += `
                <tr>
                    <td><code style="background: rgba(0,0,0,0.2); padding: 4px 8px; border-radius: 4px;">${t.token}</code></td>
                    <td><span class="role-badge role-${t.role.replace('_', '')}">${t.role}</span></td>
                    <td style="font-size: 0.8rem; color: var(--text-muted);">${t.timestamp || t.created_at}</td>
                    <td>
                        ${!isSelf ? `<button onclick="deleteToken('${t.token}')" style="background: var(--accent-red); padding: 5px 10px; font-size: 0.75rem; width: auto; border-radius: 6px;"><i class="fas fa-trash"></i></button>` : '<span style="font-size: 0.7rem; color: var(--accent-green);">Active Session</span>'}
                    </td>
                </tr>
            `;
        });
    } catch (e) {}
}

async function generateNewToken() {
    const role = document.getElementById('gen-token-role').value;
    try {
        const response = await authFetch(`${API_URL}/api/tokens/generate`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ role })
        });
        
        if (response.ok) {
            const data = await response.json();
            const display = document.getElementById('new-token-display');
            const input = document.getElementById('generated-token-id');
            display.style.display = 'block';
            input.value = data.token;
            loadTokens();
        }
    } catch (e) {}
}

async function copyGeneratedToken() {
    const input = document.getElementById('generated-token-id');
    input.select();
    document.execCommand('copy');
    alert("Token kopyalandı!");
}

async function deleteToken(token) {
    if (!confirm("Bu tokeni silmek istediğinize emin misiniz?")) return;
    try {
        const response = await authFetch(`${API_URL}/api/tokens/${token}`, { method: 'DELETE' });
        if (response.ok) {
            loadTokens();
        }
    } catch (e) {}
}

// Initialize Auth on Load
window.addEventListener('load', checkAuth);

function logout() {
    if (confirm("Oturumu kapatmak istediğinize emin misiniz?")) {
        localStorage.removeItem('dash_token');
        window.location.reload();
    }
}

function filterCommands() {
    const searchTerm = document.getElementById('command-search').value.toLowerCase();
    const filteredBySearch = allCommands.filter(cmd => 
        cmd.name.toLowerCase().includes(searchTerm) || 
        cmd.description.toLowerCase().includes(searchTerm)
    );
    
    let finalFiltered = filteredBySearch;
    if (currentCategory !== 'all') {
        finalFiltered = filteredBySearch.filter(cmd => cmd.category === currentCategory);
    }
    
    renderCommandCards(finalFiltered);
}

// Kurallar (Rules) Management
async function loadRules() {
    console.log("Loading rules...");
    const guildId = await getCurrentGuildId();
    if (!guildId) return;

    loadChannels('rules-channel-select', guildId);

    try {
        const response = await authFetch(`${API_URL}/api/guild/${guildId}/rules`);
        const rules = await response.json();
        const list = document.getElementById('active-rules-list');
        list.innerHTML = '';
        rules.forEach((rule, index) => {
            list.innerHTML += `
                <div style="padding: 1rem; background: var(--bg-darker); border-radius: 10px; margin-bottom: 10px; border: 1px solid var(--border);">
                    <strong>${index + 1}. ${rule.title}</strong>
                    <p class="command-desc">${rule.content}</p>
                </div>
            `;
        });
    } catch (e) {}
}

async function saveRule() {
    const guildId = await getCurrentGuildId();
    const title = document.getElementById('rule-title').value;
    const content = document.getElementById('rule-desc').value;

    if (!title || !content) return alert("Lütfen tüm alanları doldurun.");

    try {
        await authFetch(`${API_URL}/api/guild/${guildId}/rules`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ title, content })
        });
        alert("Kural kaydedildi ve Discord'a gönderildi!");
        loadRules();
    } catch (e) {
        alert("Hata oluştu.");
    }
}

// Öneriler (Suggestions) Management
async function loadSuggestions() {
    const guildId = await getCurrentGuildId();
    if (!guildId) return;
    loadChannels('suggestions-channel-select', guildId);

    try {
        const response = await authFetch(`${API_URL}/api/guild/${guildId}/suggestions`);
        const suggestions = await response.json();
        const list = document.getElementById('suggestions-list');
        list.innerHTML = '';
        
        if (suggestions.length === 0) {
            list.innerHTML = '<p style="color: var(--text-muted); text-align: center; padding: 1rem;">Henüz öneri bulunmuyor.</p>';
            return;
        }

        suggestions.forEach(s => {
            let statusColor = 'var(--accent-orange)';
            if (s.status === 'approved') statusColor = 'var(--accent-green)';
            if (s.status === 'rejected') statusColor = 'var(--accent-red)';

            list.innerHTML += `
                <div style="padding: 1rem; background: rgba(0,0,0,0.2); border-radius: 12px; border: 1px solid var(--border);">
                    <div style="display: flex; justify-content: space-between; margin-bottom: 5px;">
                        <strong>Öneri #${s.id}</strong>
                        <span style="color: ${statusColor}; font-size: 0.8rem; text-transform: uppercase; font-weight: bold;">${s.status}</span>
                    </div>
                    <p style="font-size: 0.9rem;">"${s.content}"</p>
                    
                    ${s.status === 'pending' ? `
                    <div style="display: flex; gap: 8px; margin-top: 12px;">
                        <button onclick="approveSuggestion(${s.id})" style="background: var(--accent-green); padding: 4px 12px; font-size: 0.75rem; width: fit-content; border-radius: 6px;">Onayla</button>
                        <button onclick="rejectSuggestion(${s.id})" style="background: var(--accent-red); padding: 4px 12px; font-size: 0.75rem; width: fit-content; border-radius: 6px;">Reddet</button>
                    </div>
                    ` : ''}
                </div>
            `;
        });
    } catch (e) {}
}

async function approveSuggestion(id) {
    if (!confirm("Bu öneriyi onaylamak istediğinize emin misiniz?")) return;
    try {
        await authFetch(`${API_URL}/api/suggestion/${id}/approve`, { method: 'POST' });
        loadSuggestions();
    } catch (e) {}
}

async function rejectSuggestion(id) {
    if (!confirm("Bu öneriyi reddetmek istediğinize emin misiniz?")) return;
    try {
        await authFetch(`${API_URL}/api/suggestion/${id}/reject`, { method: 'POST' });
        loadSuggestions();
    } catch (e) {}
}

// Başvuru (Applications) Management
async function loadApplications() {
    const guildId = await getCurrentGuildId();
    if (!guildId) return;
    loadChannels('apply-channel-select', guildId);

    try {
        const response = await authFetch(`${API_URL}/api/guild/${guildId}/applications`);
        const apps = await response.json();
        const list = document.getElementById('apply-logs-container');
        
        if (!list) return;

        if (apps.length === 0) {
            list.innerHTML = '<div class="card"><h3>Gelen Başvurular</h3><p style="color: var(--text-muted); text-align: center; padding: 2rem;">Henüz yeni bir başvuru bulunmuyor.</p></div>';
            return;
        }

        list.innerHTML = '<h3 style="margin-bottom: 15px; margin-left: 10px;">Gelen Başvurular</h3>';
        apps.forEach(a => {
            let statusColor = 'var(--text-muted)';
            if (a.status === 'approved') statusColor = 'var(--accent-green)';
            if (a.status === 'rejected') statusColor = 'var(--accent-red)';

            list.innerHTML += `
                <div style="padding: 1rem; background: var(--bg-darker); border-radius: 12px; margin-top: 10px; border: 1px solid var(--border);">
                    <div style="display: flex; justify-content: space-between; align-items: flex-start;">
                        <div>
                            <strong>Başvuru #${a.id}</strong>
                            <div style="font-size: 0.75rem; color: var(--text-muted);">Tip: ${a.type}</div>
                        </div>
                        <span style="color: ${statusColor}; font-weight: 700; font-size: 0.8rem; text-transform: uppercase;">${a.status}</span>
                    </div>
                    <p style="margin-top: 10px; font-size: 0.9rem; background: rgba(255,255,255,0.03); padding: 10px; border-radius: 8px;">${a.content.replace(/\n/g, '<br>')}</p>
                    
                    ${a.status === 'pending' ? `
                    <div style="display: flex; gap: 10px; margin-top: 15px;">
                        <button onclick="approveApp(${a.id})" style="background: var(--accent-green); flex: 1; padding: 8px;" class="action-btn-main">Onayla</button>
                        <button onclick="rejectApp(${a.id})" style="background: var(--accent-red); flex: 1; padding: 8px;" class="action-btn-main">Reddet</button>
                    </div>
                    ` : ''}
                </div>
            `;
        });
    } catch (e) {}
}

async function approveApp(id) {
    if (!confirm("Bu başvuruyu onaylamak ve kullanıcıya DM göndermek istediğinize emin misiniz?")) return;
    try {
        const response = await authFetch(`${API_URL}/api/application/${id}/approve`, { method: 'POST' });
        if (response.ok) {
            alert("Başvuru onaylandı ve kullanıcıya DM gönderildi!");
            loadApplications();
        }
    } catch (e) { alert("Hata oluştu."); }
}

async function rejectApp(id) {
    if (!confirm("Bu başvuruyu reddetmek ve kullanıcıya DM göndermek istediğinize emin misiniz?")) return;
    try {
        const response = await authFetch(`${API_URL}/api/application/${id}/reject`, { method: 'POST' });
        if (response.ok) {
            alert("Başvuru reddedildi ve kullanıcıya DM gönderildi!");
            loadApplications();
        }
    } catch (e) { alert("Hata oluştu."); }
}



// Helpers
async function getCurrentGuildId() {
    // Basitlik için ilk sunucuyu alalım
    try {
        const response = await authFetch(`${API_URL}/api/servers`);
        const servers = await response.json();
        return servers.length > 0 ? servers[0].id : null;
    } catch (e) { return null; }
}

async function loadChannels(selectId, guildId) {
    const select = document.getElementById(selectId);
    if (!select) return;
    try {
        const response = await authFetch(`${API_URL}/api/guild/${guildId}/channels`);
        const channels = await response.json();
        
        // Mevcut seçimi al
        const configResp = await authFetch(`${API_URL}/api/guild/${guildId}/config`);
        const config = await configResp.json();
        let targetId = null;
        if (selectId.includes('rules')) targetId = config.rules_channel;
        if (selectId.includes('suggestions')) targetId = config.suggestions_channel;
        if (selectId.includes('apply')) targetId = config.applications_channel;

        select.innerHTML = '';
        channels.forEach(c => {
            select.innerHTML += `<option value="${c.id}" ${c.id === targetId ? 'selected' : ''}>#${c.name}</option>`;
        });
    } catch (e) {}
}

async function saveChannelConfig() {
    const guildId = await getCurrentGuildId();
    const rules = document.getElementById('rules-channel-select')?.value;
    const suggestions = document.getElementById('suggestions-channel-select')?.value;
    const apps = document.getElementById('apply-channel-select')?.value;

    try {
        await authFetch(`${API_URL}/api/guild/${guildId}/config`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                rules_channel: rules,
                suggestions_channel: suggestions,
                applications_channel: apps
            })
        });
        alert("Kanal ayarları başarıyla güncellendi!");
    } catch (e) {
        alert("Hata oluştu.");
    }
}

async function toggleApplications(checkbox) {
    const guildId = await getCurrentGuildId();
    const status = checkbox.checked;
    console.log(`Setting application status for guild ${guildId} to ${status}`);
    // API endpoint could be added here if needed, or tied to global config
}

async function sendApplyForm() {
    const guildId = await getCurrentGuildId();
    try {
        const response = await authFetch(`${API_URL}/api/guild/${guildId}/send_apply`, {
            method: 'POST'
        });
        if (response.ok) {
            alert("Başvuru yazısı başarıyla kanala gönderildi!");
        } else {
            const error = await response.json();
            alert("Hata: " + error.detail);
        }
    } catch (e) {
        alert("Bağlantı hatası.");
    }
}




async function loadTicketConfig() {
    const guildId = await getCurrentGuildId();
    if (!guildId) return;

    // Load Dropdowns
    await loadChannels('ticket-log-select', guildId);
    await loadChannels('ticket-setup-channel-select', guildId);
    
    // Load Categories
    try {
        const catResp = await authFetch(`${API_URL}/api/guild/${guildId}/categories`);
        const categories = await catResp.json();
        const catSelect = document.getElementById('ticket-category-select');
        catSelect.innerHTML = '';
        categories.forEach(c => {
            catSelect.innerHTML += `<option value="${c.id}">${c.name}</option>`;
        });

        // Load Roles
        const roleResp = await authFetch(`${API_URL}/api/guild/${guildId}/roles`);
        const roles = await roleResp.json();
        const roleSelect = document.getElementById('ticket-staff-select');
        roleSelect.innerHTML = '<option value="">Seçilmedi</option>';
        roles.forEach(r => {
            roleSelect.innerHTML += `<option value="${r.id}">${r.name}</option>`;
        });

        // Load Current Config
        const configResp = await authFetch(`${API_URL}/api/guild/${guildId}/config`);
        const config = await configResp.json();
        
        if (config.ticket_category) catSelect.value = config.ticket_category;
        if (config.ticket_staff_role) roleSelect.value = config.ticket_staff_role;
        if (config.ticket_log_channel) document.getElementById('ticket-log-select').value = config.ticket_log_channel;
        if (config.ticket_logo_url) document.getElementById('ticket-logo-input').value = config.ticket_logo_url;

    } catch (e) {
        console.error("Error loading ticket config:", e);
    }
}

async function saveTicketConfig() {
    const guildId = await getCurrentGuildId();
    const config = {
        ticket_category: document.getElementById('ticket-category-select').value,
        ticket_staff_role: document.getElementById('ticket-staff-select').value,
        ticket_log_channel: document.getElementById('ticket-log-select').value,
        ticket_logo_url: document.getElementById('ticket-logo-input').value
    };

    try {
        const response = await authFetch(`${API_URL}/api/guild/${guildId}/config`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(config)
        });
        if (response.ok) alert("Ticket ayarları başarıyla kaydedildi!");
    } catch (e) { alert("Hata oluştu."); }
}

async function sendTicketSetup() {
    const guildId = await getCurrentGuildId();
    const channelId = document.getElementById('ticket-setup-channel-select').value;
    
    try {
        const response = await authFetch(`${API_URL}/api/guild/${guildId}/send_ticket_setup?channel_id=${channelId}`, {
            method: 'POST'
        });
        if (response.ok) alert("Ticket kurulum mesajı başarıyla gönderildi!");
        else alert("Hata oluştu. Önce ayarları kaydedin.");
    } catch (e) { alert("Bağlantı hatası."); }
}

// ------ YENİ SİSTEMLER İÇİN AYAR FONKSİYONLARI ------
async function fetchRolesAndChannels(guildId) {
    const rolesResp = await authFetch(`${API_URL}/api/guild/${guildId}/roles`);
    const channelsResp = await authFetch(`${API_URL}/api/guild/${guildId}/channels`);
    const catsResp = await authFetch(`${API_URL}/api/guild/${guildId}/categories`);
    
    return {
        roles: await rolesResp.json(),
        channels: await channelsResp.json(),
        categories: await catsResp.json()
    };
}

function populateSelect(selectId, items, placeholder = "Seçilmedi") {
    const select = document.getElementById(selectId);
    if (!select) return;
    select.innerHTML = `<option value="">${placeholder}</option>`;
    items.forEach(i => {
        select.innerHTML += `<option value="${i.id}">${i.name}</option>`;
    });
}

async function loadSpecificLogs(logType, elementId) {
    const body = document.getElementById(elementId);
    if (!body) return;
    body.innerHTML = '<tr><td colspan="3" style="text-align:center;">Loading...</td></tr>';
    try {
        const response = await authFetch(`${API_URL}/api/logs?type=${logType}`);
        const logs = await response.json();
        body.innerHTML = '';
        if (logs.length === 0) {
            body.innerHTML = '<tr><td colspan="3" style="text-align:center; color: var(--text-muted);">Henüz kayıt bulunmuyor.</td></tr>';
            return;
        }
        logs.forEach(log => {
            body.innerHTML += `
                <tr>
                    <td style="padding: 1rem; font-size: 0.85rem;">${log.timestamp}</td>
                    <td style="padding: 1rem; font-size: 0.85rem;">${log.username}</td>
                    <td style="padding: 1rem; font-size: 0.85rem; color: var(--text-muted);">${log.content}</td>
                </tr>
            `;
        });
    } catch (e) {}
}

// --- EKİP ---
async function loadEkipConfig() {
    const guildId = await getCurrentGuildId();
    if (!guildId) return;
    const { roles, channels, categories } = await fetchRolesAndChannels(guildId);
    
    populateSelect('ekip-category-select', categories);
    populateSelect('ekip-staff-select', roles);
    populateSelect('ekip-log-channel-select', channels);
    populateSelect('ekip-setup-channel-select', channels);

    try {
        const configResp = await authFetch(`${API_URL}/api/guild/${guildId}/config`);
        const config = await configResp.json();
        
        if (config.ekip_category) document.getElementById('ekip-category-select').value = config.ekip_category;
        if (config.ekip_staff_role) document.getElementById('ekip-staff-select').value = config.ekip_staff_role;
        if (config.ekip_log_channel) document.getElementById('ekip-log-channel-select').value = config.ekip_log_channel;
    } catch (e) {}

    // Load Active Teams for Deletion Select
    try {
        const teamResp = await authFetch(`${API_URL}/api/guild/${guildId}/active-teams`);
        const teams = await teamResp.json();
        const deleteSelect = document.getElementById('ekip-delete-select');
        if (deleteSelect) {
            deleteSelect.innerHTML = '<option value="">Silmek için bir ekip seçin...</option>';
            teams.forEach(t => {
                deleteSelect.innerHTML += `<option value="${t.id}">${t.ekip_ismi}</option>`;
            });
        }
    } catch (e) {}

    loadSpecificLogs('ekip-log', 'ekip-logs-body');
}

async function saveEkipConfig() {
    const guildId = await getCurrentGuildId();
    const config = {
        ekip_category: document.getElementById('ekip-category-select').value,
        ekip_staff_role: document.getElementById('ekip-staff-select').value,
        ekip_log_channel: document.getElementById('ekip-log-channel-select').value
    };
    try {
        const response = await authFetch(`${API_URL}/api/guild/${guildId}/config`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(config)
        });
        if (response.ok) alert("Ekip ayarları kaydedildi!");
    } catch (e) { alert("Hata oluştu."); }
}

async function sendEkipSetup() {
    const guildId = await getCurrentGuildId();
    const channelId = document.getElementById('ekip-setup-channel-select').value;
    if(!channelId) return alert("Kanal seçmelisiniz.");
    try {
        const response = await authFetch(`${API_URL}/api/guild/${guildId}/send_ekip_setup?channel_id=${channelId}`, { method: 'POST' });
        if (response.ok) alert("Ekip kurulum mesajı gönderildi!");
    } catch (e) { alert("Hata."); }
}

async function deleteTeamFromWeb() {
    const guildId = await getCurrentGuildId();
    const teamId = document.getElementById('ekip-delete-select').value;
    
    if (!teamId) return alert("Lütfen silmek istediğiniz ekibi seçin.");
    
    if (!confirm("DİKKAT! Bu ekip tamamen silinecek (kanallar ve roller dahil). Onaylıyor musunuz?")) return;
    
    try {
        const response = await authFetch(`${API_URL}/api/guild/${guildId}/delete_team?team_id=${teamId}`, {
            method: 'POST'
        });
        
        if (response.ok) {
            alert("Ekip başarıyla silindi!");
            loadEkipConfig(); // Listeyi yenile
        } else {
            const err = await response.json();
            alert("Hata: " + err.detail);
        }
    } catch (e) {
        alert("Bağlantı hatası.");
    }
}

// --- YAYINCI ---
async function loadYayinciConfig() {
    const guildId = await getCurrentGuildId();
    if (!guildId) return;
    const { roles, channels } = await fetchRolesAndChannels(guildId);
    
    populateSelect('yayinci-channel-select', channels);
    populateSelect('yayinci-setup-channel-select', channels);
    populateSelect('yayinci-role-select', roles);

    try {
        const configResp = await authFetch(`${API_URL}/api/guild/${guildId}/config`);
        const config = await configResp.json();
        
        if (config.yayinci_channel) document.getElementById('yayinci-channel-select').value = config.yayinci_channel;
        if (config.yayinci_role) document.getElementById('yayinci-role-select').value = config.yayinci_role;
    } catch (e) {}

    loadSpecificLogs('yayinci-log', 'yayinci-logs-body');
}

async function saveYayinciConfig() {
    const guildId = await getCurrentGuildId();
    const config = {
        yayinci_channel: document.getElementById('yayinci-channel-select').value,
        yayinci_role: document.getElementById('yayinci-role-select').value
    };
    try {
        const response = await authFetch(`${API_URL}/api/guild/${guildId}/config`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(config)
        });
        if (response.ok) alert("Yayıncı ayarları kaydedildi!");
    } catch (e) { alert("Hata oluştu."); }
}

async function sendYayinciSetup() {
    const guildId = await getCurrentGuildId();
    const channelId = document.getElementById('yayinci-setup-channel-select').value;
    if(!channelId) return alert("Kanal seçmelisiniz.");
    try {
        const response = await authFetch(`${API_URL}/api/guild/${guildId}/send_yayinci_setup?channel_id=${channelId}`, { method: 'POST' });
        if (response.ok) alert("Yayıncı kontrol paneli gönderildi!");
    } catch (e) { alert("Hata."); }
}

// --- UYARI ---
async function loadUyariConfig() {
    const guildId = await getCurrentGuildId();
    if (!guildId) return;
    const { roles, channels } = await fetchRolesAndChannels(guildId);
    
    populateSelect('uyari-log-channel-select', channels);
    populateSelect('uyari-staff-select', roles);

    try {
        const configResp = await authFetch(`${API_URL}/api/guild/${guildId}/config`);
        const config = await configResp.json();
        
        if (config.uyari_log_channel) document.getElementById('uyari-log-channel-select').value = config.uyari_log_channel;
        if (config.uyari_staff_role) document.getElementById('uyari-staff-select').value = config.uyari_staff_role;
    } catch (e) {}

    loadSpecificLogs('moderation-log', 'uyari-logs-body');
}

async function saveUyariConfig() {
    const guildId = await getCurrentGuildId();
    const config = {
        uyari_log_channel: document.getElementById('uyari-log-channel-select').value,
        uyari_staff_role: document.getElementById('uyari-staff-select').value
    };
    try {
        const response = await authFetch(`${API_URL}/api/guild/${guildId}/config`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(config)
        });
        if (response.ok) alert("Uyarı ayarları kaydedildi!");
    } catch (e) { alert("Hata oluştu."); }
}

async function loadStats() {
    try {
        const response = await authFetch(`${API_URL}/api/stats`);
        const stats = await response.json();

        // Update Overview Stats
        document.getElementById('stat-guilds').textContent = stats.guild_count;
        document.getElementById('stat-users').textContent = stats.user_count;
        document.getElementById('stat-commands').textContent = stats.online ? 'Active' : 'Offline';

        // Update Top Bar
        document.getElementById('top-ping').textContent = `${stats.latency}ms`;
        document.getElementById('top-uptime').textContent = stats.uptime || '0m';
        document.getElementById('top-memory').textContent = `${stats.memory || stats.ram.toFixed(2)} MB`;

        // Update Health Progress Bars
        updateProgress('ping', (stats.latency / 500) * 100, `${stats.latency} ms`);
        updateProgress('ram', stats.ram, `${stats.ram.toFixed(2)} MB`);
        
        // Yeni: Canlı İstatistikleri Yükle (Aktif Üye, Ses vb.)
        loadLiveStats();
        
    } catch (error) {
        console.error('Stats fetch error:', error);
    }
}

function updateProgress(id, value, label) {
    const bar = document.getElementById(`${id}-progress`);
    const valText = document.getElementById(`health-${id}-val`);
    
    if (bar) {
        const percent = Math.min(Math.max(value, 0), 100);
        bar.style.width = `${percent}%`;
    }
    if (valText) {
        valText.textContent = label;
    }
}

let allCommands = [];

// Commands Management
async function loadCommands() {
    const container = document.getElementById('command-cards-container');
    if (!container) return;
    container.innerHTML = '<p>Loading commands...</p>';

    try {
        const response = await authFetch(`${API_URL}/api/commands`);
        allCommands = await response.json();
        filterCommands(); // This will handle category and search
    } catch (error) {
        container.innerHTML = '<p>Error loading commands.</p>';
    }
}

function renderCommandCards(commands) {
    const container = document.getElementById('command-cards-container');
    if (!container) return;
    container.innerHTML = '';
    
    if (commands.length === 0) {
        container.innerHTML = '<p style="grid-column: 1/-1; text-align: center; padding: 2rem; color: var(--text-muted);">No commands found.</p>';
        return;
    }

    commands.forEach(cmd => {
        const card = document.createElement('div');
        card.className = 'cmd-small-tile';
        
        let accentColor = 'var(--primary)';
        let categoryIcon = 'fa-terminal';
        
        if (cmd.category === 'Moderation') { accentColor = 'var(--accent-red)'; categoryIcon = 'fa-shield-alt'; }
        if (cmd.category === 'Utility') { accentColor = 'var(--accent-blue)'; categoryIcon = 'fa-tools'; }
        if (cmd.category === 'Economy') { accentColor = 'var(--accent-green)'; categoryIcon = 'fa-coins'; }
        
        card.style.setProperty('--primary', accentColor);

        card.innerHTML = `
            <div class="icon-box" style="color: ${accentColor}; background: ${accentColor}11; margin-bottom: 5px;">
                <i class="fas ${categoryIcon}"></i>
            </div>
            <div class="command-name" style="font-size: 0.95rem; margin-bottom: 2px;">!${cmd.name.replace('!', '')}</div>
            <div class="command-desc" style="font-size: 0.75rem; margin-bottom: 10px;">${cmd.description}</div>
            <div style="margin-top: auto;">
                <label class="switch">
                    <input type="checkbox" ${cmd.status ? 'checked' : ''} onchange="toggleCommand('${cmd.name}', this.checked)">
                    <span class="slider"></span>
                </label>
            </div>
        `;
        container.appendChild(card);
    });
}

async function toggleCommand(name, status) {
    try {
        await authFetch(`${API_URL}/api/commands/toggle`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name, status })
        });
    } catch (error) {
        alert('Failed to update command status.');
    }
}

// Logs
async function loadLogs() {
    const body = document.getElementById('logs-body');
    if (!body) return;
    body.innerHTML = '<tr><td colspan="3" style="text-align:center; padding:1rem;">Loading...</td></tr>';

    try {
        const response = await authFetch(`${API_URL}/api/logs`);
        const logs = await response.json();
        
        body.innerHTML = '';
        // Filter out ticket logs if there are many, or just show all for now
        const systemLogs = logs.filter(l => l.type !== 'ticket-log');
        
        if (systemLogs.length === 0) {
            body.innerHTML = '<tr><td colspan="3" style="text-align:center; padding:1rem; color: var(--text-muted);">Henüz sistem kaydı bulunmuyor.</td></tr>';
            return;
        }

        systemLogs.forEach(log => {
            let badge = '';
            if (log.type === 'ekip-log') badge = '<span style="background: var(--accent-blue); padding: 2px 6px; border-radius: 4px; color: white; font-size: 0.7rem; margin-right: 8px;">Ekip Sistemi</span>';
            else if (log.type === 'yayinci-log') badge = '<span style="background: var(--accent-purple); padding: 2px 6px; border-radius: 4px; color: white; font-size: 0.7rem; margin-right: 8px;">Yayıncı Sistemi</span>';
            else if (log.type === 'moderation-log') badge = '<span style="background: var(--accent-orange); padding: 2px 6px; border-radius: 4px; color: white; font-size: 0.7rem; margin-right: 8px;">Uyarı Modülü</span>';
            else if (log.type) badge = `<span style="background: var(--bg-dark); padding: 2px 6px; border-radius: 4px; color: white; font-size: 0.7rem; margin-right: 8px; border: 1px solid var(--border);">${log.type}</span>`;

            body.innerHTML += `
                <tr>
                    <td style="padding: 1rem; font-size: 0.85rem;">${log.timestamp}</td>
                    <td style="padding: 1rem; font-size: 0.85rem;">${log.username}</td>
                    <td style="padding: 1rem; font-size: 0.85rem; color: var(--text-muted);">${badge}${log.content}</td>
                </tr>
            `;
        });
    } catch (error) {}
}

async function loadTicketLogs() {
    const body = document.getElementById('ticket-logs-body');
    if (!body) return;
    body.innerHTML = '<tr><td colspan="3" style="text-align:center; padding:1rem;">Loading...</td></tr>';

    try {
        const response = await authFetch(`${API_URL}/api/logs?type=ticket-log`);
        const logs = await response.json();
        
        body.innerHTML = '';
        if (logs.length === 0) {
            body.innerHTML = '<tr><td colspan="3" style="text-align:center; padding:1rem; color: var(--text-muted);">Henüz bilet kaydı bulunmuyor.</td></tr>';
            return;
        }

        logs.forEach(log => {
            body.innerHTML += `
                <tr>
                    <td style="padding: 1rem;">${log.timestamp}</td>
                    <td style="padding: 1rem;">${log.username}</td>
                    <td style="padding: 1rem;">
                        <span style="color: var(--primary); cursor: pointer; text-decoration: underline;" onclick="viewTranscript(${log.id})">View transcript</span>
                    </td>
                </tr>
            `;
        });
    } catch (error) {}
}

// Settings
async function loadSettings() {
    try {
        const response = await authFetch(`${API_URL}/api/settings`);
        const config = await response.json();
        if (document.getElementById('bot-prefix-input')) document.getElementById('bot-prefix-input').value = config.prefix;
        if (document.getElementById('bot-activity-input')) document.getElementById('bot-activity-input').value = config.activity;
    } catch (error) {}
}

async function saveGlobalSettings() {
    const prefix = document.getElementById('bot-prefix-input').value;
    const activity = document.getElementById('bot-activity-input').value;

    try {
        const response = await authFetch(`${API_URL}/api/settings`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ prefix, activity })
        });
        if (response.ok) alert('Settings saved successfully!');
    } catch (error) {}
}

// Auto Refresh
loadStats();
setInterval(loadStats, 5000);

async function viewTranscript(logId) {
    const modal = document.getElementById('transcript-modal');
    const content = document.getElementById('transcript-content');
    
    modal.style.display = 'flex';
    content.textContent = 'Yükleniyor...';

    try {
        const response = await fetch(`${API_URL}/api/log/${logId}`);
        const log = await response.json();
        content.textContent = log.content || 'İçerik bulunamadı.';
    } catch (error) {
        content.textContent = 'Hata: Transcript yüklenemedi.';
    }
}

function closeModal() {
    document.getElementById('transcript-modal').style.display = 'none';
}

// Close modal when clicking outside
window.onclick = function(event) {
    const modal = document.getElementById('transcript-modal');
    if (event.target == modal) {
        closeModal();
    }
}

// Auto-Responder Management
async function loadAutoResponders() {
    const guildId = await getCurrentGuildId();
    if (!guildId) return;

    try {
        const response = await authFetch(`${API_URL}/api/guild/${guildId}/auto-responders`);
        const responders = await response.json();
        const body = document.getElementById('auto-responder-list-body');
        if (!body) return;
        
        body.innerHTML = '';
        if (responders.length === 0) {
            body.innerHTML = '<tr><td colspan="3" style="text-align:center; padding: 2rem; color: var(--text-muted);">Henüz otomatik cevap eklenmemiş.</td></tr>';
            return;
        }

        responders.forEach(ar => {
            body.innerHTML += `
                <tr>
                    <td style="font-weight: 500; color: var(--primary);">"${ar.keyword}"</td>
                    <td style="font-size: 0.9rem; color: var(--text-muted);">${ar.response}</td>
                    <td style="text-align: right;">
                        <button onclick="deleteAutoResponder(${ar.id})" style="background: var(--accent-red); padding: 5px 10px; width: auto; font-size: 0.75rem;"><i class="fas fa-trash"></i> Sil</button>
                    </td>
                </tr>
            `;
        });
    } catch (e) {}
}

async function addAutoResponder() {
    const guildId = await getCurrentGuildId();
    const keyword = document.getElementById('ar-keyword').value;
    const response = document.getElementById('ar-response').value;

    if (!keyword || !response) return alert("Lütfen anahtar kelime ve cevap alanlarını doldurun.");

    try {
        const resp = await authFetch(`${API_URL}/api/guild/${guildId}/auto-responders`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ keyword, response })
        });

        if (resp.ok) {
            alert("Otomatik cevap başarıyla eklendi!");
            document.getElementById('ar-keyword').value = '';
            document.getElementById('ar-response').value = '';
            loadAutoResponders();
        } else {
            alert("Hata oluştu.");
        }
    } catch (e) { alert("Hata oluştu."); }
}

async function deleteAutoResponder(id) {
    if (!confirm("Bu otomatik cevabı silmek istediğinize emin misiniz?")) return;
    try {
        const response = await authFetch(`${API_URL}/api/auto-responders/${id}`, { method: 'DELETE' });
        if (response.ok) {
            loadAutoResponders();
        }
    } catch (e) { alert("Hata oluştu."); }
}

// Embed Builder Logic
async function loadEmbedBuilder() {
    const guildId = await getCurrentGuildId();
    if (guildId) {
        loadChannels('emb-channel-select', guildId);
    }
    updateEmbedPreview();
}

function updateEmbedPreview() {
    const title = document.getElementById('emb-title').value;
    const description = document.getElementById('emb-description').value;
    const color = document.getElementById('emb-color').value;
    const author = document.getElementById('emb-author').value;
    const footer = document.getElementById('emb-footer').value;
    const image = document.getElementById('emb-image').value;
    const thumbnail = document.getElementById('emb-thumbnail').value;

    // Update hex display
    document.getElementById('emb-color-hex').value = color.toUpperCase();

    // Update preview elements
    const previewBox = document.getElementById('preview-embed-box');
    const previewTitle = document.getElementById('preview-title');
    const previewDesc = document.getElementById('preview-description');
    const previewAuthorBox = document.getElementById('preview-author-box');
    const previewAuthorName = document.getElementById('preview-author-name');
    const previewFooterBox = document.getElementById('preview-footer-box');
    const previewFooterText = document.getElementById('preview-footer-text');
    const previewImageBox = document.getElementById('preview-image-box');
    const previewImage = document.getElementById('preview-image');
    const previewThumbnail = document.getElementById('preview-thumbnail');

    previewBox.style.borderLeftColor = color;
    previewTitle.textContent = title || "Mesaj Başlığı";
    previewDesc.textContent = description || "Buraya yazdığınız içerikler canlı olarak Discord görünümüyle burada belirecektir.";

    if (author) {
        previewAuthorBox.style.display = 'flex';
        previewAuthorName.textContent = author;
    } else {
        previewAuthorBox.style.display = 'none';
    }

    if (footer) {
        previewFooterBox.style.display = 'flex';
        previewFooterText.textContent = footer;
    } else {
        previewFooterBox.style.display = 'none';
    }

    if (image) {
        previewImageBox.style.display = 'block';
        previewImage.src = image;
    } else {
        previewImageBox.style.display = 'none';
    }

    if (thumbnail) {
        previewThumbnail.style.display = 'block';
        previewThumbnail.src = thumbnail;
    } else {
        previewThumbnail.style.display = 'none';
    }
}

async function sendEmbedToDiscord() {
    const guildId = await getCurrentGuildId();
    const data = {
        channel_id: document.getElementById('emb-channel-select').value,
        title: document.getElementById('emb-title').value,
        description: document.getElementById('emb-description').value,
        color: document.getElementById('emb-color').value,
        author_name: document.getElementById('emb-author').value,
        footer_text: document.getElementById('emb-footer').value,
        image_url: document.getElementById('emb-image').value,
        thumbnail_url: document.getElementById('emb-thumbnail').value
    };

    if (!data.channel_id) return alert("Lütfen bir kanal seçin.");
    if (!data.title && !data.description) return alert("Başlık veya açıklama alanlarından en az biri dolu olmalıdır.");

    try {
        const response = await authFetch(`${API_URL}/api/guild/${guildId}/send_embed`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data)
        });

        if (response.ok) {
            alert("Zengin mesaj (Embed) başarıyla gönderildi!");
        } else {
            const err = await response.json();
            alert("Hata: " + err.detail);
        }
    } catch (e) {
        alert("Bağlantı hatası!");
    }
}

// --- Analytics & Console Logic ---

async function updateAnalyticsChart() {
    const days = document.getElementById('analytics-days')?.value || 7;
    
    // Sunucu istatistiklerini çek
    const response = await authFetch(`${API_URL}/api/guild/all/analytics?days=${days}`);
    if (!response.ok) return;
    
    const data = await response.json();
    const ctx = document.getElementById('analyticsChart').getContext('2d');

    const labels = data.map(d => d.date);
    const joins = data.map(d => d.joins);
    const leaves = data.map(d => d.leaves);
    const messages = data.map(d => d.messages);

    if (analyticsChart) {
        analyticsChart.destroy();
    }

    analyticsChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'Joins',
                    data: joins,
                    borderColor: '#2ecc71',
                    backgroundColor: 'rgba(46, 204, 113, 0.1)',
                    fill: true,
                    tension: 0.4
                },
                {
                    label: 'Leaves',
                    data: leaves,
                    borderColor: '#e74c3c',
                    backgroundColor: 'rgba(231, 76, 60, 0.1)',
                    fill: true,
                    tension: 0.4
                },
                {
                    label: 'Messages',
                    data: messages,
                    borderColor: '#5865f2',
                    backgroundColor: 'rgba(88, 101, 242, 0.1)',
                    fill: true,
                    tension: 0.4,
                    yAxisID: 'y1'
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true,
                    grid: { color: 'rgba(255,255,255,0.05)' },
                    ticks: { color: '#8e9297' }
                },
                y1: {
                    position: 'right',
                    beginAtZero: true,
                    grid: { display: false },
                    ticks: { color: '#5865f2' }
                },
                x: {
                    grid: { color: 'rgba(255,255,255,0.05)' },
                    ticks: { color: '#8e9297' }
                }
            },
            plugins: {
                legend: {
                    labels: { color: '#fff', usePointStyle: true }
                }
            }
        }
    });
}

function startConsoleStream() {
    if (consoleInterval) clearInterval(consoleInterval);
    updateWebConsole();
    consoleInterval = setInterval(updateWebConsole, 3000);
}

function stopConsoleStream() {
    if (consoleInterval) {
        clearInterval(consoleInterval);
        consoleInterval = null;
    }
}

async function updateWebConsole() {
    const consoleEl = document.getElementById('web-console');
    if (!consoleEl) return;

    try {
        const response = await authFetch(`${API_URL}/api/bot/console`);
        if (!response.ok) return;
        
        const logs = await response.json();
        consoleEl.innerHTML = ''; 

        if (logs.length === 0) {
            consoleEl.innerHTML = '<div class="console-line system">Henüz veri yok...</div>';
            return;
        }

        logs.forEach(log => {
            const line = document.createElement('div');
            line.className = `console-line ${log.type.split('-')[0]}`;
            
            const timeSpan = document.createElement('span');
            timeSpan.className = 'time';
            timeSpan.textContent = `[${new Date(log.timestamp).toLocaleTimeString()}]`;
            
            line.appendChild(timeSpan);
            line.appendChild(document.createTextNode(` ${log.message}`));
            
            consoleEl.appendChild(line);
        });
    } catch (e) {
        console.error("Console Update Error:", e);
    }
}


// Üye Yönetimi
async function loadMembers() {
    const guildId = await getCurrentGuildId();
    if (!guildId) return;

    const searchTerm = document.getElementById('member-search').value;
    const body = document.getElementById('members-list-body');
    
    try {
        const response = await authFetch(`${API_URL}/api/guild/${guildId}/members?search=${encodeURIComponent(searchTerm)}`);
        const members = await response.json();
        
        body.innerHTML = '';
        members.forEach(m => {
            const roleBadges = m.roles.map(r => `<span class="badge" style="background: rgba(255,255,255,0.1);">${r.name}</span>`).join('');
            
            body.innerHTML += `
                <tr>
                    <td>
                        <div style="display: flex; align-items: center;">
                            <img src="${m.avatar}" class="member-avatar">
                            <div>
                                <div style="font-weight: 500;">${m.display_name}</div>
                                <div style="font-size: 0.75rem; color: var(--text-muted);">@${m.name}</div>
                            </div>
                        </div>
                    </td>
                    <td><span class="role-badge" style="background: var(--primary);">${m.top_role}</span></td>
                    <td style="font-size: 0.85rem; color: var(--text-muted);">${m.joined_at}</td>
                    <td>
                        <button class="action-btn btn-orange" onclick="performMemberAction('timeout', '${m.id}')" title="Sustur"><i class="fas fa-clock"></i></button>
                        <button class="action-btn btn-red" onclick="performMemberAction('kick', '${m.id}')" title="At"><i class="fas fa-user-minus"></i></button>
                        <button class="action-btn btn-red" style="background: #96281b;" onclick="performMemberAction('ban', '${m.id}')" title="Yasakla"><i class="fas fa-hammer"></i></button>
                    </td>
                </tr>
            `;
        });
    } catch (e) {
        console.error("Member load error:", e);
    }
}

async function performMemberAction(action, userId) {
    const guildId = await getCurrentGuildId();
    if (!guildId) return;

    let reason = prompt(`${action.toUpperCase()} işlemi için sebep belirtin:`, "Kural ihlali");
    if (reason === null) return;

    let duration = null;
    if (action === 'timeout') {
        let durStr = prompt("Süre (Dakika):", "10");
        if (!durStr) return;
        duration = parseInt(durStr);
    }

    try {
        const response = await authFetch(`${API_URL}/api/member/${guildId}/${userId}/${action}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ reason, duration })
        });

        if (response.ok) {
            alert("İşlem başarıyla gerçekleştirildi.");
            loadMembers();
        } else {
            const err = await response.json();
            alert(`Hata: ${err.detail}`);
        }
    } catch (e) {
        alert("Bağlantı hatası oluştu.");
    }
}

// Bot Durumu & Kontrol
async function updateBotStatus() {
    const status = document.getElementById('bot-status-select').value;
    const activity_type = document.getElementById('bot-activity-type').value;
    const activity_name = document.getElementById('bot-activity-input').value;

    try {
        const response = await authFetch(`${API_URL}/api/bot/status`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ status, activity_type, activity_name })
        });

        if (response.ok) {
            alert("Bot durumu başarıyla güncellendi.");
        } else {
            alert("Güncelleme sırasında bir hata oluştu.");
        }
    } catch (e) {
        alert("Bağlantı hatası!");
    }
}

async function restartBot() {
    if (!confirm("Botu yeniden başlatmak istediğinize emin misiniz? Yaklaşık 10-15 saniye çevrimdışı kalacaktır.")) return;

    try {
        const response = await authFetch(`${API_URL}/api/bot/restart`, { method: 'POST' });
        if (response.ok) {
            alert("Yeniden başlatma komutu gönderildi. Lütfen bekleyin...");
            setTimeout(() => window.location.reload(), 5000);
        }
    } catch (e) {
        alert("Hata oluştu veya bot kapandı.");
    }
}

// --- LIVE STATS ---
async function loadLiveStats() {
    const guildId = await getCurrentGuildId();
    if (!guildId) return;

    try {
        const response = await authFetch(`${API_URL}/api/guild/${guildId}/live-stats`);
        const stats = await response.json();
        
        const onlineEl = document.getElementById('live-online');
        const voiceEl = document.getElementById('live-voice');
        
        if (onlineEl) onlineEl.textContent = stats.online;
        if (voiceEl) voiceEl.textContent = stats.voice;
        
        const totalEl = document.getElementById('stat-users');
        if (totalEl) totalEl.textContent = stats.total;
    } catch (e) {}
}

// --- AUTO-MOD ---
async function loadAutoMod() {
    const guildId = await getCurrentGuildId();
    if (!guildId) return;

    try {
        const response = await authFetch(`${API_URL}/api/guild/${guildId}/config`);
        const config = await response.json();
        
        document.getElementById('automod-links-toggle').checked = !!config.automod_links;
        document.getElementById('automod-spam-toggle').checked = !!config.automod_spam;
        document.getElementById('automod-words-input').value = config.automod_words || "";
    } catch (e) {}
}

async function saveAutoMod() {
    const guildId = await getCurrentGuildId();
    const data = {
        automod_links: document.getElementById('automod-links-toggle').checked ? 1 : 0,
        automod_spam: document.getElementById('automod-spam-toggle').checked ? 1 : 0,
        automod_words: document.getElementById('automod-words-input').value
    };

    try {
        const resp = await authFetch(`${API_URL}/api/guild/${guildId}/automod`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data)
        });
        if (resp.ok) alert("Auto-Mod ayarları kaydedildi!");
    } catch (e) { alert("Hata oluştu."); }
}

// --- INVITES ---
async function loadInviteLeaderboard() {
    const guildId = await getCurrentGuildId();
    if (!guildId) return;

    const body = document.getElementById('invite-leaderboard-body');
    body.innerHTML = '<tr><td colspan="3" style="text-align:center;">Yükleniyor...</td></tr>';

    try {
        const response = await authFetch(`${API_URL}/api/guild/${guildId}/invites`);
        const invites = await response.json();
        
        body.innerHTML = '';
        if (invites.length === 0) {
            body.innerHTML = '<tr><td colspan="3" style="text-align:center;">Kayıt bulunamadı.</td></tr>';
            return;
        }

        invites.forEach((inv, index) => {
            body.innerHTML += `
                <tr>
                    <td><div style="display:flex; align-items:center; gap:10px;"><span style="color:var(--text-muted); font-weight:bold;">#${index+1}</span> ${inv.user_id}</div></td>
                    <td><span class="value" style="color:var(--accent-green); font-weight:bold;">${inv.count}</span> Davet</td>
                    <td><span class="role-badge role-staff">Aktif</span></td>
                </tr>
            `;
        });
    } catch (e) {}
}

// --- GIVEAWAY ---
async function loadActiveGiveaways() {
    const guildId = await getCurrentGuildId();
    if (!guildId) return;

    // Load channels first
    loadChannels('gw-channel-select', guildId);

    const list = document.getElementById('active-giveaways-list');
    list.innerHTML = 'Yükleniyor...';

    try {
        const response = await authFetch(`${API_URL}/api/guild/${guildId}/giveaways`);
        const giveaways = await response.json();
        
        list.innerHTML = '';
        if (giveaways.length === 0) {
            list.innerHTML = '<p style="color:var(--text-muted); text-align:center; padding:2rem;">Şu an aktif bir çekiliş yok.</p>';
            return;
        }

        giveaways.forEach(gw => {
            list.innerHTML += `
                <div class="gw-item">
                    <div class="gw-info">
                        <h4>${gw.prize}</h4>
                        <p><i class="fas fa-users"></i> ${gw.winners} Kazanan | <i class="fas fa-clock"></i> ${gw.end_time}</p>
                    </div>
                    <span class="gw-status ${gw.status === 'active' ? 'active' : 'ended'}">${gw.status.toUpperCase()}</span>
                </div>
            `;
        });
    } catch (e) {}
}

async function startGiveaway() {
    const guildId = await getCurrentGuildId();
    const data = {
        prize: document.getElementById('gw-prize').value,
        winners: parseInt(document.getElementById('gw-winners').value),
        duration: document.getElementById('gw-duration').value,
        channel_id: document.getElementById('gw-channel-select').value
    };

    if (!data.prize || !data.duration || !data.channel_id) return alert("Lütfen tüm alanları doldurun.");

    try {
        const resp = await authFetch(`${API_URL}/api/guild/${guildId}/giveaways`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data)
        });
        if (resp.ok) {
            alert("Çekiliş başarıyla başlatıldı!");
            loadActiveGiveaways();
        } else {
            const err = await resp.json();
            alert("Hata: " + err.detail);
        }
    } catch (e) { alert("Bağlantı hatası."); }
}
