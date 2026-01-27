// Credential Manager Web Editor - JavaScript Application

(function() {
    'use strict';

    // DOM Elements
    const elements = {
        // Credentials table
        credentialsTable: document.getElementById('credentials-table'),
        credentialsBody: document.getElementById('credentials-body'),
        credentialsLoading: document.getElementById('credentials-loading'),
        noCredentials: document.getElementById('no-credentials'),

        // Notification
        notification: document.getElementById('notification'),

        // Buttons
        btnNewCredential: document.getElementById('btn-new-credential'),
        btnCloseModal: document.getElementById('btn-close-modal'),
        btnCancel: document.getElementById('btn-cancel'),
        btnSubmit: document.getElementById('btn-submit'),
        btnCloseTestModal: document.getElementById('btn-close-test-modal'),
        btnCloseTest: document.getElementById('btn-close-test'),

        // Modal
        credentialModal: document.getElementById('credential-modal'),
        modalTitle: document.getElementById('modal-title'),
        credentialForm: document.getElementById('credential-form'),

        // Form fields
        formMode: document.getElementById('form-mode'),
        formOriginalName: document.getElementById('form-original-name'),
        formName: document.getElementById('form-name'),
        formFqdn: document.getElementById('form-fqdn'),
        formPort: document.getElementById('form-port'),
        formOrgId: document.getElementById('form-org-id'),
        formApiUser: document.getElementById('form-api-user'),
        formApiKey: document.getElementById('form-api-key'),
        formVerifySsl: document.getElementById('form-verify-ssl'),
        formEncrypt: document.getElementById('form-encrypt'),
        formSshKey: document.getElementById('form-ssh-key'),
        formUseWorkdir: document.getElementById('form-use-workdir'),

        // Form sections
        encryptionSection: document.getElementById('encryption-section'),
        sshKeysSection: document.getElementById('ssh-keys-section'),
        storageSection: document.getElementById('storage-section'),
        apiKeyRequired: document.getElementById('api-key-required'),
        apiKeyHint: document.getElementById('api-key-hint'),

        // Test modal
        testModal: document.getElementById('test-modal'),
        testResult: document.getElementById('test-result'),

        // Delete modal
        deleteModal: document.getElementById('delete-modal'),
        deleteCredentialName: document.getElementById('delete-credential-name'),
        btnCloseDeleteModal: document.getElementById('btn-close-delete-modal'),
        btnCancelDelete: document.getElementById('btn-cancel-delete'),
        btnConfirmDelete: document.getElementById('btn-confirm-delete')
    };

    // State
    let sshKeys = [];
    let encryptionAvailable = false;
    let credentialToDelete = null;

    // Initialize the application
    async function init() {
        await loadCredentials();
        await checkEncryptionStatus();
        setupEventListeners();
    }

    // Setup event listeners
    function setupEventListeners() {
        // New credential button
        elements.btnNewCredential.addEventListener('click', () => openCreateModal());

        // Modal close buttons
        elements.btnCloseModal.addEventListener('click', closeModal);
        elements.btnCancel.addEventListener('click', closeModal);

        // Form submission
        elements.credentialForm.addEventListener('submit', handleFormSubmit);

        // Encryption checkbox
        elements.formEncrypt.addEventListener('change', toggleSshKeySelection);

        // Test modal close
        elements.btnCloseTestModal.addEventListener('click', closeTestModal);
        elements.btnCloseTest.addEventListener('click', closeTestModal);

        // Delete modal buttons
        elements.btnCloseDeleteModal.addEventListener('click', closeDeleteModal);
        elements.btnCancelDelete.addEventListener('click', closeDeleteModal);
        elements.btnConfirmDelete.addEventListener('click', confirmDelete);

        // Close modals on background click
        elements.credentialModal.addEventListener('click', (e) => {
            if (e.target === elements.credentialModal) closeModal();
        });
        elements.testModal.addEventListener('click', (e) => {
            if (e.target === elements.testModal) closeTestModal();
        });
        elements.deleteModal.addEventListener('click', (e) => {
            if (e.target === elements.deleteModal) closeDeleteModal();
        });
    }

    // API Functions
    async function apiCall(url, options = {}) {
        try {
            const response = await fetch(url, {
                headers: {
                    'Content-Type': 'application/json',
                    ...options.headers
                },
                ...options
            });
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || 'An error occurred');
            }
            return data;
        } catch (error) {
            throw error;
        }
    }

    // Load all credentials
    async function loadCredentials() {
        elements.credentialsLoading.classList.remove('hidden');
        elements.credentialsTable.classList.add('hidden');
        elements.noCredentials.classList.add('hidden');

        try {
            const credentials = await apiCall('/api/credentials');
            renderCredentials(credentials);
        } catch (error) {
            showNotification('Failed to load credentials: ' + error.message, 'error');
            elements.credentialsLoading.classList.add('hidden');
        }
    }

    // Render credentials table
    function renderCredentials(credentials) {
        elements.credentialsLoading.classList.add('hidden');

        if (credentials.length === 0) {
            elements.noCredentials.classList.remove('hidden');
            elements.credentialsTable.classList.add('hidden');
            return;
        }

        elements.credentialsTable.classList.remove('hidden');
        elements.credentialsBody.innerHTML = '';

        for (const cred of credentials) {
            const row = document.createElement('tr');
            row.innerHTML = `
                <td><strong>${escapeHtml(cred.name)}</strong></td>
                <td>${escapeHtml(cred.fqdn)}</td>
                <td>${cred.port}</td>
                <td>${cred.org_id}</td>
                <td>${escapeHtml(cred.api_user)}</td>
                <td class="${cred.api_key_encrypted ? 'status-yes' : 'status-no'}">${cred.api_key_encrypted ? 'Yes' : 'No'}</td>
                <td class="${cred.verify_ssl ? 'status-yes' : 'status-no'}">${cred.verify_ssl ? 'Yes' : 'No'}</td>
                <td title="${escapeHtml(cred.originating_file)}">${truncatePath(cred.originating_file)}</td>
                <td class="actions-cell">
                    <button class="btn btn-small btn-success btn-test" data-name="${escapeHtml(cred.name)}">Test</button>
                    <button class="btn btn-small btn-secondary btn-edit" data-name="${escapeHtml(cred.name)}">Edit</button>
                    <button class="btn btn-small btn-danger btn-delete" data-name="${escapeHtml(cred.name)}">Delete</button>
                </td>
            `;
            elements.credentialsBody.appendChild(row);
        }

        // Add event listeners to buttons
        document.querySelectorAll('.btn-test').forEach(btn => {
            btn.addEventListener('click', () => testCredential(btn.dataset.name));
        });
        document.querySelectorAll('.btn-edit').forEach(btn => {
            btn.addEventListener('click', () => openEditModal(btn.dataset.name));
        });
        document.querySelectorAll('.btn-delete').forEach(btn => {
            btn.addEventListener('click', () => openDeleteModal(btn.dataset.name));
        });
    }

    // Check encryption availability and load SSH keys
    async function checkEncryptionStatus() {
        try {
            const status = await apiCall('/api/encryption-status');
            encryptionAvailable = status.available;

            if (encryptionAvailable) {
                const keysData = await apiCall('/api/ssh-keys');
                sshKeys = keysData.keys || [];

                if (sshKeys.length > 0) {
                    elements.encryptionSection.classList.remove('hidden');
                    populateSshKeySelect();
                }
            }
        } catch (error) {
            console.log('Encryption not available:', error.message);
        }
    }

    // Populate SSH key select dropdown
    function populateSshKeySelect() {
        elements.formSshKey.innerHTML = '';
        for (const key of sshKeys) {
            const option = document.createElement('option');
            option.value = key.index;
            option.textContent = `${key.type} | ${key.fingerprint.substring(0, 16)}... | ${key.comment || 'No comment'}`;
            elements.formSshKey.appendChild(option);
        }
    }

    // Toggle SSH key selection visibility
    function toggleSshKeySelection() {
        if (elements.formEncrypt.checked && sshKeys.length > 0) {
            elements.sshKeysSection.classList.remove('hidden');
        } else {
            elements.sshKeysSection.classList.add('hidden');
        }
    }

    // Open modal for creating new credential
    function openCreateModal() {
        resetForm();
        elements.formMode.value = 'create';
        elements.modalTitle.textContent = 'New Credential';
        elements.btnSubmit.textContent = 'Create';
        elements.formName.removeAttribute('readonly');
        elements.formApiKey.setAttribute('required', 'required');
        elements.apiKeyRequired.classList.remove('hidden');
        elements.apiKeyHint.classList.add('hidden');
        elements.storageSection.classList.remove('hidden');
        elements.credentialModal.classList.remove('hidden');
        elements.formName.focus();
    }

    // Open modal for editing existing credential
    async function openEditModal(name) {
        resetForm();

        try {
            const credential = await apiCall(`/api/credentials/${encodeURIComponent(name)}`);

            elements.formMode.value = 'edit';
            elements.formOriginalName.value = credential.name;
            elements.modalTitle.textContent = 'Edit Credential';
            elements.btnSubmit.textContent = 'Update';

            // Populate form
            elements.formName.value = credential.name;
            elements.formName.setAttribute('readonly', 'readonly');
            elements.formFqdn.value = credential.fqdn;
            elements.formPort.value = credential.port;
            elements.formOrgId.value = credential.org_id;
            elements.formApiUser.value = credential.api_user;
            elements.formVerifySsl.checked = credential.verify_ssl;

            // API key is optional for updates
            elements.formApiKey.removeAttribute('required');
            elements.apiKeyRequired.classList.add('hidden');
            elements.apiKeyHint.classList.remove('hidden');

            // Hide storage section for edits
            elements.storageSection.classList.add('hidden');

            elements.credentialModal.classList.remove('hidden');
            elements.formFqdn.focus();
        } catch (error) {
            showNotification('Failed to load credential: ' + error.message, 'error');
        }
    }

    // Open modal for deleting credential
    function openDeleteModal(name) {
        credentialToDelete = name;
        elements.deleteCredentialName.textContent = name;
        elements.deleteModal.classList.remove('hidden');
    }

    // Close modal
    function closeModal() {
        elements.credentialModal.classList.add('hidden');
        resetForm();
    }

    // Close delete modal
    function closeDeleteModal() {
        elements.deleteModal.classList.add('hidden');
        credentialToDelete = null;
    }

    // Reset form
    function resetForm() {
        elements.credentialForm.reset();
        elements.formMode.value = 'create';
        elements.formOriginalName.value = '';
        elements.formPort.value = '8443';
        elements.formOrgId.value = '1';
        elements.formVerifySsl.checked = true;
        elements.formEncrypt.checked = false;
        elements.sshKeysSection.classList.add('hidden');
    }

    // Handle form submission
    async function handleFormSubmit(e) {
        e.preventDefault();

        const mode = elements.formMode.value;
        const data = {
            name: elements.formName.value.trim(),
            fqdn: elements.formFqdn.value.trim(),
            port: parseInt(elements.formPort.value),
            org_id: parseInt(elements.formOrgId.value),
            api_user: elements.formApiUser.value.trim(),
            verify_ssl: elements.formVerifySsl.checked
        };

        // Add API key if provided
        const apiKey = elements.formApiKey.value;
        if (apiKey) {
            data.api_key = apiKey;
        } else if (mode === 'create') {
            showNotification('API key is required', 'error');
            return;
        }

        // Add encryption settings
        if (elements.formEncrypt.checked && sshKeys.length > 0) {
            data.encrypt = true;
            data.ssh_key_index = parseInt(elements.formSshKey.value);
        }

        // Add storage location for create
        if (mode === 'create') {
            data.use_current_workdir = elements.formUseWorkdir.checked;
        }

        elements.btnSubmit.disabled = true;
        elements.btnSubmit.textContent = mode === 'create' ? 'Creating...' : 'Updating...';

        try {
            if (mode === 'create') {
                await apiCall('/api/credentials', {
                    method: 'POST',
                    body: JSON.stringify(data)
                });
                showNotification('Credential created successfully!', 'success');
            } else {
                const originalName = elements.formOriginalName.value;
                await apiCall(`/api/credentials/${encodeURIComponent(originalName)}`, {
                    method: 'PUT',
                    body: JSON.stringify(data)
                });
                showNotification('Credential updated successfully!', 'success');
            }

            closeModal();
            await loadCredentials();
        } catch (error) {
            showNotification('Failed to save credential: ' + error.message, 'error');
        } finally {
            elements.btnSubmit.disabled = false;
            elements.btnSubmit.textContent = mode === 'create' ? 'Create' : 'Update';
        }
    }

    // Confirm credential deletion
    async function confirmDelete() {
        if (!credentialToDelete) return;

        elements.btnConfirmDelete.disabled = true;
        elements.btnConfirmDelete.textContent = 'Deleting...';

        try {
            await apiCall(`/api/credentials/${encodeURIComponent(credentialToDelete)}`, {
                method: 'DELETE'
            });
            showNotification('Credential deleted successfully!', 'success');
            closeDeleteModal();
            await loadCredentials();
        } catch (error) {
            showNotification('Failed to delete credential: ' + error.message, 'error');
        } finally {
            elements.btnConfirmDelete.disabled = false;
            elements.btnConfirmDelete.textContent = 'Delete';
        }
    }

    // Test credential connection
    async function testCredential(name) {
        elements.testResult.className = 'test-result loading';
        elements.testResult.textContent = 'Testing connection...';
        elements.testModal.classList.remove('hidden');

        try {
            const result = await apiCall(`/api/credentials/${encodeURIComponent(name)}/test`, {
                method: 'POST'
            });
            elements.testResult.className = 'test-result success';
            elements.testResult.textContent = `Connection to "${name}" successful!`;
        } catch (error) {
            elements.testResult.className = 'test-result error';
            elements.testResult.textContent = `Connection failed: ${error.message}`;
        }
    }

    // Close test modal
    function closeTestModal() {
        elements.testModal.classList.add('hidden');
    }

    // Show notification
    function showNotification(message, type = 'info') {
        elements.notification.textContent = message;
        elements.notification.className = `notification ${type}`;
        elements.notification.classList.remove('hidden');

        // Auto-hide after 5 seconds
        setTimeout(() => {
            elements.notification.classList.add('hidden');
        }, 5000);
    }

    // Utility: Escape HTML
    function escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    // Utility: Truncate file path for display
    function truncatePath(path) {
        if (path.length > 30) {
            return '...' + path.slice(-27);
        }
        return path;
    }

    // Start the application
    document.addEventListener('DOMContentLoaded', init);
})();
