/**
 * Entity card widget: BI-style config for Main and Secondary values.
 * Each value has its own data set (entity, fields, filters) + selected field + formula.
 * Depends on window.__cardDeps set by the main entity-table widget.
 */
(function() {
    'use strict';

    var REPORT_CARD_ICONS = ['iconoir-activity', 'iconoir-handbag', 'iconoir-clock', 'iconoir-user', 'iconoir-dollar-circle', 'iconoir-report-columns', 'iconoir-reports-solid', 'iconoir-trophy', 'iconoir-chat-bubble', 'iconoir-mail', 'iconoir-fire-flame', 'iconoir-suitcase', 'iconoir-bell', 'iconoir-calendar', 'iconoir-list', 'iconoir-candlestick-chart', 'iconoir-map-pin', 'iconoir-send-mail', 'iconoir-page-star', 'iconoir-heart-solid'];

    function defaultValueConfig() {
        return {
            selectedEntities: [],
            selectedFields: [],
            filterFields: [{ label: 'Название', key: '', type: 'text', value: '' }],
            selectedFieldKey: null,
            aggregate: 'count'
        };
    }

    function defaultCardState() {
        return {
            type: 'card',
            cardTitle: '',
            icon: 'iconoir-activity',
            mainValueConfig: (function() {
                var c = defaultValueConfig();
                return c;
            })(),
            secondaryValueConfig: (function() {
                var c = defaultValueConfig();
                c.label = '';
                return c;
            })(),
            fullData: [],
            startNewRow: false
        };
    }

    function normalizeSearchText(s) {
        var out = String(s || '').toLowerCase().trim();
        if (out.normalize) out = out.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
        return out;
    }

    function buildFilterFieldMaps(key, filterFields, fieldsCache) {
        var filterFieldMaps = [];
        var cache = fieldsCache[key];
        if (cache && cache.fields && Array.isArray(cache.fields)) {
            (filterFields || []).forEach(function(f) {
                if (!f || !f.label) return;
                var ht = String(f.label || '').trim();
                if (!ht) return;
                var aliases = [];
                cache.fields.forEach(function(meta) {
                    if (!meta) return;
                    if (normalizeSearchText(String(meta.human_title || '')) === normalizeSearchText(ht)) {
                        var cn = String(meta.column_name || '').trim();
                        var b24 = String(meta.b24_field || '').trim();
                        if (cn && aliases.indexOf(cn) < 0) aliases.push(cn);
                        if (b24 && aliases.indexOf(b24) < 0) aliases.push(b24);
                    }
                });
                if (aliases.length) filterFieldMaps.push({ title: ht, aliases: aliases });
            });
        }
        return filterFieldMaps;
    }

    function filterRowsByFilterFields(rows, key, filterFields) {
        if (!rows || !filterFields || !filterFields.length) return rows;
        return rows.filter(function(row) {
            return filterFields.every(function(f) {
                if (!f || !f.label) return true;
                var targetKey = (f.key && f.key.indexOf('::') >= 0) ? f.key : (key + '::' + f.label);
                var val = row[targetKey];
                if (f.selectedValues && f.selectedValues.length) return f.selectedValues.some(function(sv) { return String(val) === String(sv); });
                if (f.value != null && String(f.value).trim() !== '') return String(val || '').toLowerCase().indexOf(String(f.value).trim().toLowerCase()) >= 0;
                return true;
            });
        });
    }

    function computeAggregate(rows, config, rowKeys) {
        if (!rows || !rows.length) return null;
        var agg = (config && config.aggregate) ? config.aggregate : 'count';
        if (agg === 'count') return rows.length;
        var fieldKey = (config && config.selectedFieldKey) ? config.selectedFieldKey : null;
        if (!fieldKey || !rowKeys || rowKeys.indexOf(fieldKey) < 0) return rows.length;
        var nums = [];
        rows.forEach(function(r) {
            var v = r[fieldKey];
            if (v == null || v === '') return;
            if (typeof v === 'number' && !isNaN(v)) { nums.push(v); return; }
            var s = String(v).trim().replace(/\s/g, '').replace(',', '.');
            var n = parseFloat(s);
            if (!isNaN(n)) nums.push(n);
        });
        if (nums.length === 0) return null;
        if (agg === 'sum') return nums.reduce(function(a, b) { return a + b; }, 0);
        if (agg === 'avg') return nums.reduce(function(a, b) { return a + b; }, 0) / nums.length;
        if (agg === 'min') return Math.min.apply(null, nums);
        if (agg === 'max') return Math.max.apply(null, nums);
        return null;
    }

    function loadOneCardDataset(config, deps, done) {
        if (!config.selectedEntities || !config.selectedEntities.length) {
            done(null, []);
            return;
        }
        var ent = config.selectedEntities[0];
        var key = deps.entityKey(ent);
        var sel = config.selectedFields.find(function(f) { return f.entityKey === key; });
        var titles = (sel && sel.human_titles && sel.human_titles.length) ? sel.human_titles : [];
        if (titles.length === 0) {
            done(null, []);
            return;
        }
        var fieldsCache = deps.getFieldsCache ? deps.getFieldsCache() : {};
        var filterFieldMaps = buildFilterFieldMaps(key, config.filterFields, fieldsCache);
        deps.fetchPage(ent, titles, key, 0, 10000, filterFieldMaps).then(function(res) {
            var rows = (res && res.rows) ? res.rows : [];
            rows = filterRowsByFilterFields(rows, key, config.filterFields);
            done(rows, null);
        }).catch(function() {
            done(null, new Error('fetch failed'));
        });
    }

    function loadCardData(cardIndex) {
        var deps = window.__cardDeps;
        if (!deps) return;
        var tableStates = deps.getTableStates();
        var st = tableStates[cardIndex];
        if (!st || st.type !== 'card') return;
        var getSectionEls = deps.getSectionEls;
        if (!getSectionEls) return;

        var mainCfg = st.mainValueConfig;
        var secCfg = st.secondaryValueConfig;
        if (!mainCfg) mainCfg = { selectedEntities: st.selectedEntities || [], selectedFields: st.selectedFields || [], filterFields: st.filterFields || [], selectedFieldKey: (st.mainFormula && st.mainFormula.fieldKey) || null, aggregate: (st.mainFormula && st.mainFormula.aggregate) || 'count' };
        if (!secCfg) secCfg = { selectedEntities: st.selectedEntities || [], selectedFields: st.selectedFields || [], filterFields: st.filterFields || [], selectedFieldKey: (st.secondaryFormula && st.secondaryFormula.fieldKey) || null, aggregate: (st.secondaryFormula && st.secondaryFormula.aggregate) || 'count', label: (st.secondaryFormula && st.secondaryFormula.label) || '' };

        var els = getSectionEls(cardIndex);
        var setMain = function(val) {
            if (els.mainValueEl) els.mainValueEl.textContent = (val != null) ? (typeof val === 'number' && (val % 1 !== 0 || val > 1e9) ? val.toLocaleString('ru-RU', { maximumFractionDigits: 2 }) : val) : '—';
        };
        var setSec = function(val, label) {
            if (els.secondaryValueEl) {
                if (val != null) els.secondaryValueEl.textContent = (label && label.trim()) ? (label.trim() + ': ' + val) : String(val);
                else els.secondaryValueEl.textContent = (label && label.trim()) ? label.trim() : '';
            }
        };

        loadOneCardDataset(mainCfg, deps, function(mainRows, err) {
            var mainVal = null;
            if (!err && mainRows && mainRows.length) {
                var rowKeys = Object.keys(mainRows[0]);
                mainVal = computeAggregate(mainRows, { selectedFieldKey: mainCfg.selectedFieldKey, aggregate: mainCfg.aggregate }, rowKeys);
            }
            setMain(mainVal);

            loadOneCardDataset(secCfg, deps, function(secRows, errSec) {
                var secVal = null;
                if (!errSec && secRows && secRows.length) {
                    var secRowKeys = Object.keys(secRows[0]);
                    secVal = computeAggregate(secRows, { selectedFieldKey: secCfg.selectedFieldKey, aggregate: secCfg.aggregate }, secRowKeys);
                }
                setSec(secVal, (secCfg.label != null) ? secCfg.label : '');
            });
        });
    }

    function updateCardUI(cardIndex) {
        var deps = window.__cardDeps;
        if (!deps) return;
        var tableStates = deps.getTableStates();
        var st = tableStates[cardIndex];
        if (!st || st.type !== 'card') return;
        var els = deps.getSectionEls(cardIndex);
        if (!els || !els.section) return;
        if (els.cardTitleEl) els.cardTitleEl.textContent = (st.cardTitle && st.cardTitle.trim()) ? st.cardTitle.trim() : 'Карточка';
        if (els.iconEl) els.iconEl.className = (st.icon || 'iconoir-activity') + ' fs-4';
        if (els.iconWrap) els.iconWrap.className = 'report-card-icon-wrap bg-primary-subtle text-primary';
        loadCardData(cardIndex);
    }

    function parseCardStateFromSaved(t) {
        var norm = (window.__cardDeps && window.__cardDeps.normalizeEntitiesAndFields) ? window.__cardDeps.normalizeEntitiesAndFields(t.entities || [], t.fields || []) : { entities: [], fields: [] };
        var mainCfg = (t.main_value_config && typeof t.main_value_config === 'object') ? {
            selectedEntities: (t.main_value_config.entities && t.main_value_config.entities.length) ? (window.__cardDeps && window.__cardDeps.normalizeEntitiesAndFields(t.main_value_config.entities, t.main_value_config.fields || [])).entities : [],
            selectedFields: (t.main_value_config.entities && t.main_value_config.entities.length) ? (window.__cardDeps && window.__cardDeps.normalizeEntitiesAndFields(t.main_value_config.entities, t.main_value_config.fields || [])).fields : [],
            filterFields: (t.main_value_config.filter_fields && Array.isArray(t.main_value_config.filter_fields)) ? t.main_value_config.filter_fields : [{ label: 'Название', key: '', type: 'text', value: '' }],
            selectedFieldKey: (t.main_value_config.selected_field_key != null) ? t.main_value_config.selected_field_key : null,
            aggregate: (t.main_value_config.aggregate && typeof t.main_value_config.aggregate === 'string') ? t.main_value_config.aggregate : 'count'
        } : null;
        var secCfg = (t.secondary_value_config && typeof t.secondary_value_config === 'object') ? {
            selectedEntities: (t.secondary_value_config.entities && t.secondary_value_config.entities.length) ? (window.__cardDeps && window.__cardDeps.normalizeEntitiesAndFields(t.secondary_value_config.entities, t.secondary_value_config.fields || [])).entities : [],
            selectedFields: (t.secondary_value_config.entities && t.secondary_value_config.entities.length) ? (window.__cardDeps && window.__cardDeps.normalizeEntitiesAndFields(t.secondary_value_config.entities, t.secondary_value_config.fields || [])).fields : [],
            filterFields: (t.secondary_value_config.filter_fields && Array.isArray(t.secondary_value_config.filter_fields)) ? t.secondary_value_config.filter_fields : [{ label: 'Название', key: '', type: 'text', value: '' }],
            selectedFieldKey: (t.secondary_value_config.selected_field_key != null) ? t.secondary_value_config.selected_field_key : null,
            aggregate: (t.secondary_value_config.aggregate && typeof t.secondary_value_config.aggregate === 'string') ? t.secondary_value_config.aggregate : 'count',
            label: (t.secondary_value_config.label != null) ? String(t.secondary_value_config.label) : ''
        } : null;

        if (!mainCfg && (t.entities && t.entities.length)) {
            var mf = t.main_formula && typeof t.main_formula === 'object' ? t.main_formula : {};
            mainCfg = {
                selectedEntities: norm.entities,
                selectedFields: norm.fields,
                filterFields: (t.filter_fields && Array.isArray(t.filter_fields)) ? t.filter_fields : [{ label: 'Название', key: '', type: 'text', value: '' }],
                selectedFieldKey: (mf.field_key != null) ? mf.field_key : (mf.fieldKey != null) ? mf.fieldKey : null,
                aggregate: (mf.aggregate && typeof mf.aggregate === 'string') ? mf.aggregate : 'count'
            };
        }
        if (!secCfg && (t.entities && t.entities.length)) {
            var sf = t.secondary_formula && typeof t.secondary_formula === 'object' ? t.secondary_formula : {};
            secCfg = {
                selectedEntities: norm.entities,
                selectedFields: norm.fields,
                filterFields: (t.filter_fields && Array.isArray(t.filter_fields)) ? t.filter_fields : [{ label: 'Название', key: '', type: 'text', value: '' }],
                selectedFieldKey: (sf.field_key != null) ? sf.field_key : (sf.fieldKey != null) ? sf.fieldKey : null,
                aggregate: (sf.aggregate && typeof sf.aggregate === 'string') ? sf.aggregate : 'count',
                label: (sf.label != null) ? String(sf.label) : ''
            };
        }
        if (!mainCfg) mainCfg = defaultValueConfig();
        if (!secCfg) {
            secCfg = defaultValueConfig();
            secCfg.label = '';
        }

        return {
            type: 'card',
            cardTitle: (t.card_title != null) ? String(t.card_title) : '',
            icon: (t.icon && typeof t.icon === 'string') ? t.icon : 'iconoir-activity',
            mainValueConfig: mainCfg,
            secondaryValueConfig: secCfg,
            fullData: [],
            startNewRow: (t.start_new_row === true)
        };
    }

    function serializeCardState(st) {
        var deps = window.__cardDeps;
        var main = st.mainValueConfig;
        var sec = st.secondaryValueConfig;
        if (!main) main = { selectedEntities: [], selectedFields: [], filterFields: [], selectedFieldKey: null, aggregate: 'count' };
        if (!sec) sec = { selectedEntities: [], selectedFields: [], filterFields: [], selectedFieldKey: null, aggregate: 'count', label: '' };
        function toEntitiesFields(entities, fields) {
            if (!deps || !deps.normalizeEntitiesAndFields) return { entities: entities || [], fields: fields || [] };
            return { entities: entities || [], fields: fields || [] };
        }
        return {
            type: 'card',
            card_title: st.cardTitle,
            icon: st.icon || 'iconoir-activity',
            main_value_config: {
                entities: main.selectedEntities,
                fields: main.selectedFields,
                filter_fields: main.filterFields,
                selected_field_key: main.selectedFieldKey,
                aggregate: main.aggregate || 'count'
            },
            secondary_value_config: {
                entities: sec.selectedEntities,
                fields: sec.selectedFields,
                filter_fields: sec.filterFields,
                selected_field_key: sec.selectedFieldKey,
                aggregate: sec.aggregate || 'count',
                label: (sec.label != null) ? String(sec.label) : ''
            },
            start_new_row: (st.startNewRow === true)
        };
    }

    function setupCardModal() {
        var deps = window.__cardDeps;
        if (!deps) return;
        var modalEl = document.getElementById('modalAddCard');
        var btnSave = document.getElementById('modalAddCardSave');
        var btnDelete = document.getElementById('modalAddCardDelete');
        var cardTitleInput = document.getElementById('cardTitleInput');
        var cardIconSelect = document.getElementById('cardIconSelect');
        var cardMainEntitySelect = document.getElementById('cardMainEntitySelect');
        var cardMainFieldsWrap = document.getElementById('cardMainFieldsWrap');
        var cardMainFiltersWrap = document.getElementById('cardMainFiltersWrap');
        var cardMainAddFilter = document.getElementById('cardMainAddFilter');
        var cardMainFieldSelect = document.getElementById('cardMainFieldSelect');
        var cardMainAggregate = document.getElementById('cardMainAggregate');
        var cardSecondaryEntitySelect = document.getElementById('cardSecondaryEntitySelect');
        var cardSecondaryFieldsWrap = document.getElementById('cardSecondaryFieldsWrap');
        var cardSecondaryFiltersWrap = document.getElementById('cardSecondaryFiltersWrap');
        var cardSecondaryAddFilter = document.getElementById('cardSecondaryAddFilter');
        var cardSecondaryFieldSelect = document.getElementById('cardSecondaryFieldSelect');
        var cardSecondaryAggregate = document.getElementById('cardSecondaryAggregate');
        var cardSecondaryLabel = document.getElementById('cardSecondaryLabel');
        var modalTitleEl = document.getElementById('modalAddCardTitle');
        if (!modalEl || !btnSave) return;

        var modal = new bootstrap.Modal(modalEl);
        var editingCardIndex = null;

        if (cardIconSelect) {
            cardIconSelect.innerHTML = REPORT_CARD_ICONS.map(function(ic) {
                return '<option value="' + ic + '">' + ic + '</option>';
            }).join('');
        }

        function getEntitiesList() {
            return (deps.getEntitiesList && deps.getEntitiesList()) || [];
        }

        function fillEntitySelect(selectEl, selectedKey) {
            if (!selectEl) return;
            var list = getEntitiesList();
            var html = '<option value="">— Выберите сущность —</option>' + list.map(function(ent) {
                var key = deps.entityKey(ent);
                var title = (ent.title || ent.type || key || '').trim();
                return '<option value="' + key + '">' + title + '</option>';
            }).join('');
            selectEl.innerHTML = html;
            if (selectedKey) selectEl.value = selectedKey;
        }

        function fillFieldsCheckboxes(wrapEl, entityKeyVal, selectedTitles, which) {
            if (!wrapEl) return;
            wrapEl.innerHTML = '';
            if (!entityKeyVal) return;
            var cache = deps.getFieldsCache ? deps.getFieldsCache() : {};
            var fields = (cache[entityKeyVal] && cache[entityKeyVal].fields) ? cache[entityKeyVal].fields : [];
            var titles = [];
            fields.forEach(function(meta) {
                var ht = String(meta.human_title || '').trim();
                if (ht) titles.push(ht);
            });
            titles.forEach(function(title) {
                var key = entityKeyVal + '::' + title;
                var checked = selectedTitles && selectedTitles.indexOf(title) >= 0;
                var div = document.createElement('div');
                div.className = 'form-check';
                var cb = document.createElement('input');
                cb.type = 'checkbox';
                cb.className = 'form-check-input card-field-cb';
                cb.setAttribute('data-key', key);
                cb.setAttribute('data-title', title);
                cb.checked = checked;
                var lab = document.createElement('label');
                lab.className = 'form-check-label small';
                lab.textContent = title;
                div.appendChild(cb);
                div.appendChild(lab);
                wrapEl.appendChild(div);
            });
            if (which === 'main' && cardMainFieldSelect) updateMainFieldSelect();
            if (which === 'secondary' && cardSecondaryFieldSelect) updateSecondaryFieldSelect();
        }

        function updateMainFieldSelect() {
            if (!cardMainFieldSelect || !cardMainFieldsWrap) return;
            var opts = '<option value="">— Выберите поле —</option>';
            cardMainFieldsWrap.querySelectorAll('.card-field-cb:checked').forEach(function(cb) {
                var k = cb.getAttribute('data-key');
                var t = cb.getAttribute('data-title');
                if (k && t) opts += '<option value="' + k + '">' + t + '</option>';
            });
            cardMainFieldSelect.innerHTML = opts;
        }

        function updateSecondaryFieldSelect() {
            if (!cardSecondaryFieldSelect || !cardSecondaryFieldsWrap) return;
            var opts = '<option value="">— Выберите поле —</option>';
            cardSecondaryFieldsWrap.querySelectorAll('.card-field-cb:checked').forEach(function(cb) {
                var k = cb.getAttribute('data-key');
                var t = cb.getAttribute('data-title');
                if (k && t) opts += '<option value="' + k + '">' + t + '</option>';
            });
            cardSecondaryFieldSelect.innerHTML = opts;
        }

        function renderFilterRows(container, filterFields, which) {
            if (!container) return;
            container.innerHTML = '';
            (filterFields || []).forEach(function(f, idx) {
                var row = document.createElement('div');
                row.className = 'd-flex align-items-center gap-2 mb-2 card-filter-row';
                row.setAttribute('data-idx', idx);
                var lab = document.createElement('input');
                lab.type = 'text';
                lab.className = 'form-control form-control-sm flex-grow-1';
                lab.placeholder = 'Поле (название)';
                lab.value = (f.label != null) ? f.label : '';
                var val = document.createElement('input');
                val.type = 'text';
                val.className = 'form-control form-control-sm flex-grow-1';
                val.placeholder = 'Значение';
                val.value = (f.value != null) ? f.value : '';
                var rm = document.createElement('button');
                rm.type = 'button';
                rm.className = 'btn btn-outline-danger btn-sm';
                rm.textContent = '×';
                rm.addEventListener('click', function() {
                    filterFields.splice(idx, 1);
                    if (!filterFields.length) filterFields.push({ label: 'Название', key: '', type: 'text', value: '' });
                    renderFilterRows(container, filterFields, which);
                });
                row.appendChild(lab);
                row.appendChild(val);
                row.appendChild(rm);
                container.appendChild(row);
            });
        }

        function collectFilterRows(container) {
            var out = [];
            if (!container) return out;
            container.querySelectorAll('.card-filter-row').forEach(function(row) {
                var lab = row.querySelector('input[placeholder="Поле (название)"]');
                var val = row.querySelector('input[placeholder="Значение"]');
                var l = (lab && lab.value) ? lab.value.trim() : '';
                if (!l) return;
                out.push({ label: l, key: '', type: 'text', value: (val && val.value) ? val.value.trim() : '' });
            });
            if (!out.length) out.push({ label: 'Название', key: '', type: 'text', value: '' });
            return out;
        }

        function openAddCardModal(cardIndex) {
            editingCardIndex = cardIndex;
            if (modalTitleEl) modalTitleEl.textContent = (cardIndex != null) ? 'Настроить карточку' : 'Добавить карточку отчёта';
            if (btnDelete) btnDelete.classList.toggle('d-none', cardIndex == null);

            deps.loadEntitiesList().then(function() {
                fillEntitySelect(cardMainEntitySelect);
                fillEntitySelect(cardSecondaryEntitySelect);

                var tableStates = deps.getTableStates();
                if (cardIndex != null && tableStates[cardIndex] && tableStates[cardIndex].type === 'card') {
                    var st = tableStates[cardIndex];
                    if (cardTitleInput) cardTitleInput.value = (st.cardTitle || '').trim();
                    if (cardIconSelect) cardIconSelect.value = (st.icon || 'iconoir-activity');

                    var main = st.mainValueConfig || {};
                    if (cardMainEntitySelect && main.selectedEntities && main.selectedEntities[0]) {
                        var mk = deps.entityKey(main.selectedEntities[0]);
                        cardMainEntitySelect.value = mk;
                        deps.loadFieldsForEntity(main.selectedEntities[0]).then(function() {
                            fillFieldsCheckboxes(cardMainFieldsWrap, mk, (main.selectedFields && main.selectedFields[0]) ? main.selectedFields[0].human_titles : [], 'main');
                            if (cardMainFieldSelect) cardMainFieldSelect.value = (main.selectedFieldKey != null) ? main.selectedFieldKey : '';
                            if (cardMainAggregate) cardMainAggregate.value = (main.aggregate || 'count');
                        });
                    }
                    renderFilterRows(cardMainFiltersWrap, main.filterFields || [], 'main');

                    var sec = st.secondaryValueConfig || {};
                    if (cardSecondaryEntitySelect && sec.selectedEntities && sec.selectedEntities[0]) {
                        var sk = deps.entityKey(sec.selectedEntities[0]);
                        cardSecondaryEntitySelect.value = sk;
                        deps.loadFieldsForEntity(sec.selectedEntities[0]).then(function() {
                            fillFieldsCheckboxes(cardSecondaryFieldsWrap, sk, (sec.selectedFields && sec.selectedFields[0]) ? sec.selectedFields[0].human_titles : [], 'secondary');
                            if (cardSecondaryFieldSelect) cardSecondaryFieldSelect.value = (sec.selectedFieldKey != null) ? sec.selectedFieldKey : '';
                            if (cardSecondaryAggregate) cardSecondaryAggregate.value = (sec.aggregate || 'count');
                        });
                    }
                    if (cardSecondaryLabel) cardSecondaryLabel.value = (sec.label != null) ? sec.label : '';
                    renderFilterRows(cardSecondaryFiltersWrap, sec.filterFields || [], 'secondary');
                } else {
                    if (cardTitleInput) cardTitleInput.value = '';
                    if (cardIconSelect) cardIconSelect.value = 'iconoir-activity';
                    fillFieldsCheckboxes(cardMainFieldsWrap, null, [], 'main');
                    fillFieldsCheckboxes(cardSecondaryFieldsWrap, null, [], 'secondary');
                    renderFilterRows(cardMainFiltersWrap, [{ label: 'Название', key: '', type: 'text', value: '' }], 'main');
                    renderFilterRows(cardSecondaryFiltersWrap, [{ label: 'Название', key: '', type: 'text', value: '' }], 'secondary');
                    if (cardMainFieldSelect) cardMainFieldSelect.innerHTML = '<option value="">— Выберите поле —</option>';
                    if (cardSecondaryFieldSelect) cardSecondaryFieldSelect.innerHTML = '<option value="">— Выберите поле —</option>';
                    if (cardMainAggregate) cardMainAggregate.value = 'count';
                    if (cardSecondaryAggregate) cardSecondaryAggregate.value = 'count';
                    if (cardSecondaryLabel) cardSecondaryLabel.value = '';
                }
                modal.show();
            });
        }

        if (cardMainEntitySelect) {
            cardMainEntitySelect.addEventListener('change', function() {
                var key = this.value;
                if (!key) {
                    fillFieldsCheckboxes(cardMainFieldsWrap, null, [], 'main');
                    return;
                }
                var list = getEntitiesList();
                var ent = list.find(function(e) { return deps.entityKey(e) === key; });
                if (ent) deps.loadFieldsForEntity(ent).then(function() { fillFieldsCheckboxes(cardMainFieldsWrap, key, [], 'main'); });
                else fillFieldsCheckboxes(cardMainFieldsWrap, null, [], 'main');
            });
        }
        if (cardMainFieldsWrap) {
            cardMainFieldsWrap.addEventListener('change', function() { updateMainFieldSelect(); });
        }
        if (cardSecondaryEntitySelect) {
            cardSecondaryEntitySelect.addEventListener('change', function() {
                var key = this.value;
                if (!key) {
                    fillFieldsCheckboxes(cardSecondaryFieldsWrap, null, [], 'secondary');
                    return;
                }
                var list = getEntitiesList();
                var ent = list.find(function(e) { return deps.entityKey(e) === key; });
                if (ent) deps.loadFieldsForEntity(ent).then(function() { fillFieldsCheckboxes(cardSecondaryFieldsWrap, key, [], 'secondary'); });
                else fillFieldsCheckboxes(cardSecondaryFieldsWrap, null, [], 'secondary');
            });
        }
        if (cardSecondaryFieldsWrap) {
            cardSecondaryFieldsWrap.addEventListener('change', function() { updateSecondaryFieldSelect(); });
        }

        if (cardMainAddFilter) {
            cardMainAddFilter.addEventListener('click', function() {
                var container = cardMainFiltersWrap;
                if (!container) return;
                var div = document.createElement('div');
                div.className = 'd-flex align-items-center gap-2 mb-2 card-filter-row';
                div.innerHTML = '<input type="text" class="form-control form-control-sm flex-grow-1" placeholder="Поле (название)"><input type="text" class="form-control form-control-sm flex-grow-1" placeholder="Значение"><button type="button" class="btn btn-outline-danger btn-sm">×</button>';
                var rm = div.querySelector('button');
                rm.addEventListener('click', function() { div.remove(); });
                container.appendChild(div);
            });
        }
        if (cardSecondaryAddFilter) {
            cardSecondaryAddFilter.addEventListener('click', function() {
                var container = cardSecondaryFiltersWrap;
                if (!container) return;
                var div = document.createElement('div');
                div.className = 'd-flex align-items-center gap-2 mb-2 card-filter-row';
                div.innerHTML = '<input type="text" class="form-control form-control-sm flex-grow-1" placeholder="Поле (название)"><input type="text" class="form-control form-control-sm flex-grow-1" placeholder="Значение"><button type="button" class="btn btn-outline-danger btn-sm">×</button>';
                var rm = div.querySelector('button');
                rm.addEventListener('click', function() { div.remove(); });
                container.appendChild(div);
            });
        }

        if (btnDelete) {
            btnDelete.addEventListener('click', function() {
                if (editingCardIndex == null) return;
                if (!confirm('Удалить эту карточку?')) return;
                var idx = editingCardIndex;
                modal.hide();
                deps.deleteTableAtIndex(idx);
            });
        }

        if (btnSave) {
            btnSave.addEventListener('click', function() {
                var title = (cardTitleInput && cardTitleInput.value) ? cardTitleInput.value.trim() : '';
                if (!title) {
                    alert('Введите название карточки.');
                    if (cardTitleInput) cardTitleInput.focus();
                    return;
                }

                var mainKey = (cardMainEntitySelect && cardMainEntitySelect.value) ? cardMainEntitySelect.value : '';
                var mainEnt = mainKey ? getEntitiesList().find(function(e) { return deps.entityKey(e) === mainKey; }) : null;
                var mainTitles = [];
                if (cardMainFieldsWrap) cardMainFieldsWrap.querySelectorAll('.card-field-cb:checked').forEach(function(cb) {
                    var t = cb.getAttribute('data-title');
                    if (t) mainTitles.push(t);
                });
                if (mainTitles.length === 0 && mainEnt) mainTitles = ['ID'];
                var mainNorm = mainEnt ? deps.normalizeEntitiesAndFields([mainEnt], [{ entityKey: mainKey, human_titles: mainTitles }]) : { entities: [], fields: [] };
                var mainFieldKey = (cardMainFieldSelect && cardMainFieldSelect.value) ? cardMainFieldSelect.value : null;
                var mainFilterFields = collectFilterRows(cardMainFiltersWrap);
                mainFilterFields.forEach(function(f) {
                    if (f.label && mainKey) f.key = mainKey + '::' + f.label;
                });

                var secKey = (cardSecondaryEntitySelect && cardSecondaryEntitySelect.value) ? cardSecondaryEntitySelect.value : '';
                var secEnt = secKey ? getEntitiesList().find(function(e) { return deps.entityKey(e) === secKey; }) : null;
                var secTitles = [];
                if (cardSecondaryFieldsWrap) cardSecondaryFieldsWrap.querySelectorAll('.card-field-cb:checked').forEach(function(cb) {
                    var t = cb.getAttribute('data-title');
                    if (t) secTitles.push(t);
                });
                if (secTitles.length === 0 && secEnt) secTitles = ['ID'];
                var secNorm = secEnt ? deps.normalizeEntitiesAndFields([secEnt], [{ entityKey: secKey, human_titles: secTitles }]) : { entities: [], fields: [] };
                var secFieldKey = (cardSecondaryFieldSelect && cardSecondaryFieldSelect.value) ? cardSecondaryFieldSelect.value : null;
                var secFilterFields = collectFilterRows(cardSecondaryFiltersWrap);
                secFilterFields.forEach(function(f) {
                    if (f.label && secKey) f.key = secKey + '::' + f.label;
                });

                var cardState = {
                    type: 'card',
                    cardTitle: title || 'Карточка',
                    icon: (cardIconSelect && cardIconSelect.value) ? cardIconSelect.value : 'iconoir-activity',
                    mainValueConfig: {
                        selectedEntities: mainNorm.entities,
                        selectedFields: mainNorm.fields,
                        filterFields: mainFilterFields,
                        selectedFieldKey: mainFieldKey,
                        aggregate: (cardMainAggregate && cardMainAggregate.value) ? cardMainAggregate.value : 'count'
                    },
                    secondaryValueConfig: {
                        selectedEntities: secNorm.entities,
                        selectedFields: secNorm.fields,
                        filterFields: secFilterFields,
                        selectedFieldKey: secFieldKey,
                        aggregate: (cardSecondaryAggregate && cardSecondaryAggregate.value) ? cardSecondaryAggregate.value : 'count',
                        label: (cardSecondaryLabel && cardSecondaryLabel.value) ? cardSecondaryLabel.value.trim() : ''
                    },
                    fullData: [],
                    startNewRow: false
                };

                var tableStates = deps.getTableStates();
                if (editingCardIndex != null) {
                    tableStates[editingCardIndex] = cardState;
                    deps.ensureSections();
                    updateCardUI(editingCardIndex);
                } else {
                    tableStates.push(cardState);
                    deps.ensureSections();
                    updateCardUI(tableStates.length - 1);
                }
                deps.saveConfigToStorage();
                deps.saveConfig();
                modal.hide();
            });
        }

        window.openAddCardModal = openAddCardModal;
    }

    document.addEventListener('DOMContentLoaded', function() {
        if (!window.__cardDeps) return;
        window.EntityCard = {
            defaultCardState: defaultCardState,
            parseCardStateFromSaved: parseCardStateFromSaved,
            serializeCardState: serializeCardState,
            loadCardData: loadCardData,
            updateCardUI: updateCardUI
        };
        setupCardModal();
    });
})();
