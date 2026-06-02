async function logout() {
    await fetch('/api/logout', {method: 'POST'});
    window.location.href = '/login';
}

const _origFetch = window.fetch;
window.fetch = async function(...args) {
    const resp = await _origFetch(...args);
    if (resp.status === 401) {
        window.location.href = '/login';
    }
    return resp;
};
