// Ensure runCommand is available early so form.onsubmit handlers can reference it
async function runCommand(name, form, meta){
  const data = { command: name, args: {} };

  // first handle PCE selection / manual
  const manualEl = form.querySelector('[name="pce_manual"]');
  if (manualEl && manualEl.value) {
    data.pce = manualEl.value;
  } else {
    const pceEl = form.querySelector('[name="pce"]');
    if (pceEl && pceEl.value) data.pce = pceEl.value;
  }

  // iterate metadata to collect and coerce values properly
  for (const arg of meta.arguments) {
    const name = arg.dest;
    // check for explicit null checkbox first
    const isNullEl = form.querySelector('[name="' + name + '__isnull"]');
    if (isNullEl && isNullEl.checked) {
      data.args[name] = null;
      continue;
    }

    // try to find a checkbox first
    const checkbox = form.querySelector('[name="' + name + '"][type=checkbox]');
    if (checkbox) {
      // checkbox exists; its value is boolean
      data.args[name] = checkbox.checked;
      continue;
    }
    const el = form.elements[name];
    if (!el) continue; // not provided

    let value = el.value;
    if (value === '') {
      // treat empty as undefined to avoid sending empty strings for optional args
      continue;
    }

    // coerce numbers
    if (el.type === 'number' || arg.type === 'int' || arg.type === 'float' || arg.type === 'number') {
      const num = Number(value);
      if (!isNaN(num)) {
        data.args[name] = (arg.type === 'int') ? Math.trunc(num) : num;
      } else {
        data.args[name] = value;
      }
      continue;
    }

    // keep strings as-is
    data.args[name] = value;
  }

  document.getElementById('logs').textContent = 'Running...';
  const res = await fetch('/api/run', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data) });
  const out = await res.json();
  document.getElementById('logs').textContent = '' + (out.stdout || '') + '\n' + (out.stderr || '');
}

// Export to window immediately to make it reachable from onsubmit handlers in all scopes
try { if (typeof window !== 'undefined') window.runCommand = runCommand; } catch (e) { /* ignore */ }

function chooseOptionString(arg) {
  // prefer a long option starting with '--' from option_strings, otherwise pick first, otherwise fallback to '--' + dest
  if (!arg.option_strings || arg.option_strings.length === 0) return '--' + arg.dest;
  // prefer longest starting with --
  const longOpts = arg.option_strings.filter(s => s.startsWith('--'));
  if (longOpts.length > 0) return longOpts.sort((a,b) => b.length - a.length)[0];
  // else prefer any starting with -
  const shortOpts = arg.option_strings.filter(s => s.startsWith('-'));
  if (shortOpts.length > 0) return shortOpts[0];
  return '--' + arg.dest;
}

// modify selectCommand to remember option_strings for preview rendering
async function selectCommand(name){
  const res = await fetch('/api/commands/' + encodeURIComponent(name));
  const meta = await res.json();
  // Store option strings map on the meta object for convenience
  meta._opt_map = {};
  for (const a of meta.arguments) {
    meta._opt_map[a.dest] = chooseOptionString(a);
  }

  const formDiv = document.getElementById('command-form');
  formDiv.innerHTML = '';
  const form = document.createElement('form');
  form.onsubmit = async (e) => { e.preventDefault(); (window.runCommand || runCommand)(name, form, meta); };

  // Add form-level delegation listeners so any input/change inside the form updates the CLI preview.
  // This is a robust fallback so we don't miss updates on textboxes/selects/checkboxes regardless
  // of which individual elements have explicit listeners attached.
  try {
    form.addEventListener('input', () => updateCliPreview(form, meta));
    form.addEventListener('change', () => updateCliPreview(form, meta));
  } catch (e) { /* ignore attach errors */ }

  // pce dropdown (populated from /api/credentials)
  const pceLabel = document.createElement('label'); pceLabel.textContent = 'PCE profile (if required): ';
  const pceSelect = document.createElement('select'); pceSelect.name = 'pce';
  const loadingOption = document.createElement('option'); loadingOption.value = '';
  loadingOption.textContent = 'Loading...';
  pceSelect.appendChild(loadingOption);
  form.appendChild(pceLabel); form.appendChild(pceSelect); form.appendChild(document.createElement('br'));

  // populate credentials and if only one pre-select it
  await (async () => {
    try {
      const credRes = await fetch('/api/credentials');
      let creds;
      if (credRes.ok) {
        try { creds = await credRes.json(); } catch (e) { creds = []; }
      } else {
        // non-OK response: treat as empty list and continue to UI behavior
        creds = [];
      }
      pceSelect.innerHTML = ''; // clear loading
      // empty option for "none"
      const emptyOpt = document.createElement('option'); emptyOpt.value = ''; emptyOpt.textContent = '-- none --';
      pceSelect.appendChild(emptyOpt);
      creds.forEach(c => {
        const opt = document.createElement('option'); opt.value = c.name; opt.textContent = c.name + (c.fqdn ? (' (' + c.fqdn + ')') : ''); pceSelect.appendChild(opt);
      });
      if (creds.length === 1) {
        // auto-select sole profile
        pceSelect.value = creds[0].name;
        // update preview
        updateCliPreview(form, meta);
      }
    } catch (e) {
      console.warn('Could not load credentials list', e);
      pceSelect.innerHTML = '';
      const fallback = document.createElement('option'); fallback.value = ''; fallback.textContent = '-- none (or enter manually) --';
      pceSelect.appendChild(fallback);
      const manual = document.createElement('input'); manual.type = 'text'; manual.name = 'pce_manual'; manual.placeholder = 'Type PCE profile name';
      form.insertBefore(manual, pceSelect.nextSibling);
      form.insertBefore(document.createElement('br'), manual.nextSibling);
    }
  })();

  meta.arguments.forEach(arg => {
    const lbl = document.createElement('label'); lbl.textContent = arg.dest + ': ';
    let control;

    // If choices exist, use a select control
    if (arg.choices && Array.isArray(arg.choices) && arg.choices.length > 0) {
      control = document.createElement('select');
      control.name = arg.dest;
      const emptyOpt = document.createElement('option'); emptyOpt.value = ''; emptyOpt.textContent = '-- none --';
      control.appendChild(emptyOpt);
      arg.choices.forEach(choice => {
        const o = document.createElement('option'); o.value = choice; o.textContent = choice; control.appendChild(o);
      });
      if (arg.default !== null && arg.default !== undefined) {
        control.value = String(arg.default);
      }
    }
    // boolean defaults -> checkbox
    else if (typeof arg.default === 'boolean') {
      // hidden fallback so unchecked boxes send a value
      const hidden = document.createElement('input'); hidden.type = 'hidden'; hidden.name = arg.dest; hidden.value = 'false';
      const checkbox = document.createElement('input'); checkbox.type = 'checkbox'; checkbox.name = arg.dest; checkbox.value = 'true';
      if (arg.default === true) checkbox.checked = true;
      control = document.createElement('span');
      control.appendChild(hidden);
      control.appendChild(checkbox);
    }
    // numeric types
    else if (arg.type === 'int' || arg.type === 'float' || arg.type === 'number') {
      const input = document.createElement('input');
      input.type = 'number';
      input.name = arg.dest;
      if (arg.type === 'int') input.step = '1';
      if (arg.default !== null && arg.default !== undefined) {
        input.value = String(arg.default);
      }
      control = input;
      // For number inputs, also allow explicit null via a checkbox if default is null
      if (arg.default === null) {
        const isNull = document.createElement('input'); isNull.type = 'checkbox'; isNull.name = arg.dest + '__isnull';
        isNull.id = arg.dest + '__isnull';
        const isNullLabel = document.createElement('label'); isNullLabel.htmlFor = isNull.id; isNullLabel.textContent = ' null';
        // If default is null, check the box and disable input by default
        isNull.checked = true;
        input.value = '';
        input.disabled = true;
        isNull.addEventListener('change', () => {
          if (isNull.checked) {
            input.value = '';
            input.disabled = true;
          } else {
            input.disabled = false;
          }
          updateCliPreview(form, meta);
        });
        // wrap number and checkbox in a span
        const span = document.createElement('span');
        span.appendChild(input);
        span.appendChild(isNull);
        span.appendChild(isNullLabel);
        control = span;
      }
    }
    // fallback to text input
    else {
      const input = document.createElement('input'); input.name = arg.dest; input.type = 'text';
      input.placeholder = arg.help || '';
      if (arg.default !== null && arg.default !== undefined) {
        try { input.value = String(arg.default); } catch (e) { input.value = '' + arg.default; }
      }

      // If the argument can be null (default is explicitly null), add a 'null' checkbox that blanks and disables the input
      if (arg.default === null) {
        const isNull = document.createElement('input'); isNull.type = 'checkbox'; isNull.name = arg.dest + '__isnull';
        isNull.id = arg.dest + '__isnull';
        const isNullLabel = document.createElement('label'); isNullLabel.htmlFor = isNull.id; isNullLabel.textContent = ' null';
        // If default is null, check the box and disable the input by default
        isNull.checked = true;
        input.value = '';
        input.disabled = true;
        isNull.addEventListener('change', () => {
          if (isNull.checked) {
            input.value = '';
            input.disabled = true;
          } else {
            input.disabled = false;
          }
          updateCliPreview(form, meta);
        });
        const span = document.createElement('span');
        span.appendChild(input);
        span.appendChild(isNull);
        span.appendChild(isNullLabel);
        control = span;
      } else {
        control = input;
      }
    }

    form.appendChild(lbl); form.appendChild(control); form.appendChild(document.createElement('br'));

    // Add input/change listeners to controls to update CLI preview
    try {
      // Attach listeners to the actual input/select/textarea elements inside the control.
      // Previously we used form.querySelector('[name=...]') which could pick the hidden fallback
      // input instead of the visible checkbox (hidden input appears first), preventing change
      // events from firing when the visible control changed. Attaching directly to each element
      // avoids that problem.
      if (control && control.querySelector) {
        const els = control.querySelectorAll('input, select, textarea');
        els.forEach(el => {
          try {
            el.addEventListener('change', () => updateCliPreview(form, meta));
            el.addEventListener('input', () => updateCliPreview(form, meta));
          } catch (e) { /* ignore individual listener attach failures */ }
        });
      } else if (control && control.name) {
        // single control element (not a container)
        const el = form.querySelector('[name="' + control.name + '"]') || control;
        try {
          el.addEventListener('change', () => updateCliPreview(form, meta));
          el.addEventListener('input', () => updateCliPreview(form, meta));
        } catch (e) { /* ignore */ }
      }
    } catch (e) {
      // ignore listener attach failures
    }
  });

  const runBtn = document.createElement('button'); runBtn.textContent = 'RUN'; runBtn.type = 'submit';
  form.appendChild(runBtn);
  formDiv.appendChild(form);

  // store meta on the form so delegated handlers can access it
  try { form._meta = meta; } catch (e) { }

  // initial preview
  updateCliPreview(form, meta);
}

// Update updateCliPreview to use option strings when building CLI
function updateCliPreview(form, meta) {
  const previewEl = document.getElementById('cli-preview');
  if (!previewEl) return;
  const cmdParts = ['python', '-m', 'illumio_pylo.cli'];

  // include --pce if provided
  const manualEl = (form) ? form.querySelector('[name="pce_manual"]') : document.querySelector('#command-form input[name="pce_manual"]');
  if (manualEl && manualEl.value) {
    cmdParts.push('--pce');
    cmdParts.push(escapeArg(manualEl.value));
  } else {
    const pceEl = (form) ? form.querySelector('[name="pce"]') : document.querySelector('#command-form select[name="pce"]');
    if (pceEl && pceEl.value) {
      cmdParts.push('--pce');
      cmdParts.push(escapeArg(pceEl.value));
    }
  }

  // command name
  const selectedCommand = previewEl.getAttribute('data-command');
  if (selectedCommand) {
    cmdParts.push(selectedCommand);
  }
  if (!form || !meta) {
    previewEl.value = cmdParts.join(' ');
    try { autosizeTextarea(previewEl, 2); } catch (e) {}
    return;
  }

  for (const arg of meta.arguments) {
    const name = arg.dest;
    const opt = (meta._opt_map && meta._opt_map[name]) ? meta._opt_map[name] : ('--' + name);

    // check null checkbox first - only include if the current state differs from the default
    const isNullEl = form.querySelector('[name="' + name + '__isnull"]');
    if (isNullEl) {
      const isNullChecked = !!isNullEl.checked;
      const defaultIsNull = (arg.default === null);
      if (isNullChecked) {
        // only include explicit None when the default is NOT null (i.e. value differs from default)
        if (!defaultIsNull) {
          cmdParts.push(opt);
          cmdParts.push('None');
        }
        continue;
      }
      // if not checked, fall through and compare the actual input value below
    }

    // boolean checkboxes: only include when the checked state differs from the declared default
    const checkbox = form.querySelector('[name="' + name + '"][type=checkbox]');
    if (checkbox) {
      const checked = !!checkbox.checked;
      const defaultChecked = (typeof arg.default === 'boolean') ? arg.default : false;
      // For flag style booleans, render the flag only when the value differs from the default
      if (checked !== defaultChecked && checked === true) {
        cmdParts.push(opt);
      }
      continue;
    }

    const el = form.elements[name];
    if (!el) continue;
    let value = el.value;
    if (value === '') continue;

    const numericField = isNumericArgType(arg, el);
    if (numericField) {
      const num = Number(value);
      if (!isNaN(num)) {
        if (isArgNumberDefault(arg, num)) continue;
        cmdParts.push(opt);
        cmdParts.push(String(num));
        continue;
      }
    }

    if (Array.isArray(value)) {
      const arr = value;
      if (isArrayArgDefault(arg, arr)) continue;
      arr.forEach(v => { cmdParts.push(opt); cmdParts.push(escapeArg(v)); });
      continue;
    }

    if (isArgValueDefault(arg, value)) continue;

    cmdParts.push(opt);
    cmdParts.push(escapeArg(value));
  }

  previewEl.value = cmdParts.join(' ');
  // autosize to show at least 2 rows and expand if necessary
  try {
    autosizeTextarea(previewEl, 2);
    requestAnimationFrame(() => { try { autosizeTextarea(previewEl, 2); } catch (e) {} });
    setTimeout(() => { try { autosizeTextarea(previewEl, 2); } catch (e) {} }, 150);
  } catch (e) { }
}

// Quote/format an argument value for CLI preview
function escapeArg(v) {
  if (v === null || v === undefined) return 'None';
  if (typeof v === 'boolean') return v ? 'true' : 'false';
  if (typeof v === 'number') return String(v);
  // For strings, quote if it contains whitespace or special chars
  if (typeof v === 'string') {
    if (/\s|"|'|\\/.test(v)) {
      return '"' + v.replace(/"/g, '\\"') + '"';
    }
    return v;
  }
  try { return String(v); } catch (e) { return JSON.stringify(v); }
}

// Autosize a textarea to fit content with a minimum of 2 rows
function autosizeTextarea(el, minRows = 2) {
  if (!el) return;
  try {
    const cs = getComputedStyle(el);
    // attempt to determine a reasonable lineHeight
    let lineHeight = parseFloat(cs.lineHeight);
    if (isNaN(lineHeight)) {
      const fontSize = parseFloat(cs.fontSize) || 14;
      lineHeight = fontSize * 1.2;
    }

    // If element width is zero (hidden), schedule a retry after it's likely visible
    const width = el.clientWidth || el.offsetWidth || parseFloat(cs.width) || 0;
    if (width <= 0) {
      // try again shortly
      setTimeout(() => autosizeTextarea(el, minRows), 80);
      return;
    }

    // Create an offscreen mirror to measure wrapped height reliably
    const mirror = document.createElement('div');
    mirror.style.position = 'absolute';
    mirror.style.visibility = 'hidden';
    mirror.style.top = '0';
    mirror.style.left = '-9999px';
    mirror.style.whiteSpace = 'pre-wrap';
    mirror.style.wordWrap = 'break-word';
    mirror.style.boxSizing = cs.boxSizing;
    mirror.style.width = width + 'px';
    // copy font and spacing styles that affect layout
    mirror.style.fontFamily = cs.fontFamily;
    mirror.style.fontSize = cs.fontSize;
    mirror.style.lineHeight = cs.lineHeight;
    mirror.style.padding = cs.padding;
    mirror.style.border = cs.border;
    mirror.style.letterSpacing = cs.letterSpacing;

    // use textContent to preserve line breaks
    mirror.textContent = el.value || '';

    document.body.appendChild(mirror);
    const needed = mirror.scrollHeight;
    document.body.removeChild(mirror);

    const minHeight = Math.ceil(minRows * lineHeight);
    const final = Math.max(needed, minHeight);
    el.style.height = final + 'px';
  } catch (e) {
    // fallback: set a conservative min height
    el.style.height = (minRows * 18) + 'px';
  }
}

// Wire copy button
try {
  const copyBtn = document.getElementById('copy-cli');
  const copyStatus = document.getElementById('copy-cli-status');
  if (copyBtn) {
    copyBtn.addEventListener('click', async () => {
      const preview = document.getElementById('cli-preview');
      if (!preview) return;
      const text = preview.value || '';
      let ok;
      try {
        if (navigator.clipboard && navigator.clipboard.writeText) {
          await navigator.clipboard.writeText(text);
          ok = true;
        } else {
          preview.select();
          ok = document.execCommand('copy');
        }
      } catch (e) {
        console.warn('Copy failed', e);
        ok = false;
      }

      if (ok && copyStatus) {
        copyStatus.style.display = 'block';
        setTimeout(() => { copyStatus.style.display = 'none'; }, 2000);
      }
    });
  }
} catch (e) {}

// Populate the commands list (used on startup)
async function fetchCommands(){
  const container = document.getElementById('commands');
  if (!container) return;
  container.innerHTML = '';

  // ensure search wiring
  const search = document.getElementById('command-search');
  if (search) {
    search.addEventListener('input', () => {
      const q = (search.value || '').toLowerCase();
      const cards = container.querySelectorAll('.card');
      cards.forEach(c => {
        const name = (c.getAttribute('data-name') || '').toLowerCase();
        c.style.display = name.includes(q) ? '' : 'none';
      });
    });
  }

  try {
    const res = await fetch('/api/commands');
    let cmds = [];
    if (res.ok) {
      try { cmds = await res.json(); } catch (e) { cmds = []; }
    } else {
      const div = document.createElement('div');
      div.textContent = 'Error loading commands: HTTP ' + res.status;
      container.appendChild(div);
      return;
    }
    if (!cmds || cmds.length === 0) {
      const div = document.createElement('div');
      div.textContent = 'No commands available.';
      container.appendChild(div);
      return;
    }
    cmds.forEach(c => {
      // render as a card
      const card = document.createElement('div');
      card.className = 'card';
      card.setAttribute('tabindex', '0');
      card.setAttribute('role', 'button');
      card.setAttribute('data-name', c.name);

      const title = document.createElement('div'); title.className = 'card-title'; title.textContent = c.name;
      const desc = document.createElement('div'); desc.className = 'card-desc'; desc.textContent = (c.description || c.help || c.name);
      card.appendChild(title); card.appendChild(desc);

      function onSelect() {
        // update preview data and show params panel
        const previewEl = document.getElementById('cli-preview');
        if (previewEl) previewEl.setAttribute('data-command', c.name);
        showPanel('params');
        selectCommand(c.name);
      }

      card.addEventListener('click', onSelect);
      card.addEventListener('keydown', (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); onSelect(); } });

      container.appendChild(card);
    });
  } catch (e) {
    console.error('Failed to load commands', e);
    const div = document.createElement('div');
    div.textContent = 'Error loading commands: ' + (e && e.message ? e.message : String(e));
    container.appendChild(div);
  }
}

// Expose helpers on window in case other inline scripts reference them
try { if (typeof window !== 'undefined') { window.updateCliPreview = updateCliPreview; window.autosizeTextarea = autosizeTextarea; window.runCommand = runCommand; } } catch (e) {}

// Add missing panel & navigation helpers: showPanel, wireBackButton, attachEscapeHandler
// These are lightweight, safe implementations that match the expected behavior used by
// the DOMContentLoaded handler (showing/hiding panels, wiring the Back button, and
// closing the params panel on Escape).
function showPanel(name) {
  try {
    const paramsPanel = document.getElementById('panel-params');
    if (name === 'params') {
      document.body.classList.add('params-visible');
      if (paramsPanel) {
        paramsPanel.hidden = false;
        paramsPanel.classList.remove('hidden');
      }
      // update current command name from the preview data if present
      try {
        const preview = document.getElementById('cli-preview');
        const current = document.getElementById('current-command-name');
        if (current) {
          current.textContent = (preview && preview.getAttribute) ? (preview.getAttribute('data-command') || '(no command)') : '(no command)';
        }
      } catch (e) {}
      // focus back button
      try { const back = document.getElementById('back-btn'); if (back && back.focus) back.focus(); } catch (e) {}
    } else {
      document.body.classList.remove('params-visible');
      if (paramsPanel) {
        paramsPanel.hidden = true;
        paramsPanel.classList.add('hidden');
      }
      // focus search box if present
      try { const search = document.getElementById('command-search'); if (search && search.focus) search.focus(); } catch (e) {}
    }
  } catch (e) {
    console.warn('showPanel failed', e);
  }
}

function wireBackButton() {
  try {
    const back = document.getElementById('back-btn');
    if (!back) return;
    if (back._wired) return;
    back._wired = true;
    back.addEventListener('click', (e) => { e.preventDefault(); showPanel('landing'); });
  } catch (e) { /* ignore */ }
}

function attachEscapeHandler() {
  try {
    // ensure we only attach once
    if (window._escapeHandlerAttached) return;
    window._escapeHandlerAttached = true;

    document.addEventListener('keydown', (e) => {
      // modern check for Escape
      if (e.key === 'Escape' || e.key === 'Esc' || e.keyCode === 27) {
        try {
          const params = document.getElementById('panel-params');
          if (!params) return;
          // If params panel is visible (not hidden and not display:none), go back to landing
          const isHidden = params.hidden || (getComputedStyle(params).display === 'none');
          if (!isHidden) {
            showPanel('landing');
            e.preventDefault();
          }
        } catch (err) {
          // ignore
        }
      }
    });
  } catch (e) { /* ignore */ }
}

// ensure initial population runs after DOM is ready to avoid flash of panel-params
document.addEventListener('DOMContentLoaded', () => {
  fetchCommands();
  attachEscapeHandler();
  wireBackButton();
  // attach a document-level delegating listener as a fail-safe so typing in
  // any textboxes inside the generated form updates the CLI preview.
  try {
    if (!window._cliPreviewDelegationAttached) {
      window._cliPreviewDelegationAttached = true;
      document.addEventListener('input', (e) => {
        const formEl = document.querySelector('#command-form form');
        if (!formEl) return;
        if (!formEl.contains(e.target)) return;
        try { updateCliPreview(formEl, formEl._meta); } catch (err) { /* ignore */ }
      });
      document.addEventListener('change', (e) => {
        const formEl = document.querySelector('#command-form form');
        if (!formEl) return;
        if (!formEl.contains(e.target)) return;
        try { updateCliPreview(formEl, formEl._meta); } catch (err) { /* ignore */ }
      });
    }
  } catch (e) { /* ignore */ }
  // start on the landing panel so params stays hidden until a card is chosen
  showPanel('landing');
});

function isNumericArgType(arg, el) {
  if (el && el.type === 'number') return true;
  return arg.type === 'int' || arg.type === 'float' || arg.type === 'number';
}

function isArgNumberDefault(arg, num) {
  if (arg.default === null || arg.default === undefined) return false;
  const defaultNum = Number(arg.default);
  return !isNaN(defaultNum) && defaultNum === num;
}

function isArrayArgDefault(arg, arr) {
  if (!Array.isArray(arg.default)) return false;
  if (arr.length !== arg.default.length) return false;
  return arr.every((v, i) => String(v) === String(arg.default[i]));
}

function isArgValueDefault(arg, value) {
  if (arg.default === null || arg.default === undefined) return false;
  return String(value) === String(arg.default);
}
