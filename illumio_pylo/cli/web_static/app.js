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

  // pce dropdown (populated from /api/credentials)
  const pceLabel = document.createElement('label'); pceLabel.textContent = 'PCE profile (if required): ';
  const pceSelect = document.createElement('select'); pceSelect.name = 'pce';
  const loadingOption = document.createElement('option'); loadingOption.value = '';
  loadingOption.textContent = 'Loading...';
  pceSelect.appendChild(loadingOption);
  form.appendChild(pceLabel); form.appendChild(pceSelect); form.appendChild(document.createElement('br'));

  // populate credentials and if only one pre-select it
  (async () => {
    try {
      const credRes = await fetch('/api/credentials');
      if (!credRes.ok) throw new Error('Failed to load credentials');
      const creds = await credRes.json();
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
        isNull.addEventListener('change', (e) => {
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
      const namesToWatch = [];
      if (control.querySelector) {
        const els = control.querySelectorAll('input, select, textarea');
        els.forEach(el => namesToWatch.push(el.name));
      } else if (control.name) {
        namesToWatch.push(control.name);
      }
      namesToWatch.forEach(n => {
        const el = form.querySelector('[name="' + n + '"]');
        if (el) {
          el.addEventListener('change', () => updateCliPreview(form, meta));
          el.addEventListener('input', () => updateCliPreview(form, meta));
        }
      });
    } catch (e) {
      // ignore listener attach failures
    }
  });

  const runBtn = document.createElement('button'); runBtn.textContent = 'RUN'; runBtn.type = 'submit';
  form.appendChild(runBtn);
  formDiv.appendChild(form);

  // initial preview
  updateCliPreview(form, meta);
}

// Update updateCliPreview to use option strings when building CLI
function updateCliPreview(form, meta) {
  const previewEl = document.getElementById('cli-preview');
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
    return;
  }

  for (const arg of meta.arguments) {
    const name = arg.dest;
    const opt = (meta._opt_map && meta._opt_map[name]) ? meta._opt_map[name] : ('--' + name);

    // check null checkbox
    const isNullEl = form.querySelector('[name="' + name + '__isnull"]');
    if (isNullEl && isNullEl.checked) {
      cmdParts.push(opt);
      cmdParts.push('None');
      continue;
    }

    const checkbox = form.querySelector('[name="' + name + '"][type=checkbox]');
    if (checkbox) {
      if (checkbox.checked) {
        cmdParts.push(opt);
      }
      continue;
    }

    const el = form.elements[name];
    if (!el) continue;
    let value = el.value;
    if (value === '') continue;

    if (Array.isArray(value)) {
      value.forEach(v => cmdParts.push(opt, escapeArg(v)));
    } else {
      cmdParts.push(opt);
      cmdParts.push(escapeArg(value));
    }
  }

  previewEl.value = cmdParts.join(' ');
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

// Wire copy button
try {
  const copyBtn = document.getElementById('copy-cli');
  const copyStatus = document.getElementById('copy-cli-status');
  if (copyBtn) {
    copyBtn.addEventListener('click', async () => {
      const preview = document.getElementById('cli-preview');
      if (!preview) return;
      const text = preview.value || '';
      let ok = false;
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
  const ul = document.getElementById('commands');
  if (!ul) return;
  ul.innerHTML = '';
  try {
    const res = await fetch('/api/commands');
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const cmds = await res.json();
    if (!cmds || cmds.length === 0) {
      const li = document.createElement('li');
      li.textContent = 'No commands available.';
      ul.appendChild(li);
      return;
    }
    cmds.forEach(c => {
      const li = document.createElement('li');
      const btn = document.createElement('button');
      btn.textContent = c.name;
      btn.onclick = () => {
        selectCommand(c.name);
        // Also update the preview command directly when a command is selected
        const previewEl = document.getElementById('cli-preview');
        if (previewEl) previewEl.setAttribute('data-command', c.name);
        updateCliPreview(document.querySelector('#command-form form'), { arguments: [] }); // empty meta just to refresh
      };
      li.appendChild(btn);
      ul.appendChild(li);
    });
  } catch (e) {
    console.error('Failed to load commands', e);
    const li = document.createElement('li');
    li.textContent = 'Error loading commands: ' + (e && e.message ? e.message : String(e));
    ul.appendChild(li);
  }
}

// ensure initial population
fetchCommands();

