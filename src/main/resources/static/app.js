// Global variables
let currentPage = 1;
const itemsPerPage = 50;
let totalPages = 1;
let allLogs = [];
let currentSort = { column: 'time', direction: 'desc' };
let statusChart, timeChart;
let parsingInterval = null;
let selectedFile = null;
let startTime = null; // используется для оценки оставшегося времени
const totalStages = 4; // используется в updateProgress
// Глобальные переменные для управления запросами
let isRequestInProgress = false;
let currentAbortController = null;
let requestStartTime = null;
let requestStatusTimeout = null;
let activeRequestType = null;

const STAGE_WEIGHTS = {
    COUNTING_LINES: 0.05,      // 5%
    PARSING: 0.30,            // 30%
    FINALIZATION: 0.20,       // 20%
    INDEXING: 0.30,           // 30%
    STATISTICS: 0.15          // 15%
};

const AVG_STAGE_TIMES = {
    FINALIZATION: 10000,      // 10 секунд (среднее)
    INDEXING: 30000,          // 30 секунд (среднее)
    STATISTICS: 15000         // 15 секунд (среднее)
};

// ДОБАВИТЬ после других глобальных переменных
let currentStage = null;
let stageStartTime = null;
let stageProgress = 0;
let totalProgress = 0;
let stageEstimates = {};

// API endpoints
const API_ENDPOINTS = {
    LOGS: '/api/logs',
    TOP_URLS: '/api/top-urls',
    TOP_USERS: '/api/top-users',
    START_PARSING: '/api/start-file-parsing',
    PARSING_STATUS: '/api/parsing-status',
    CANCEL_PARSING: '/api/cancel-parsing',
    CHECK_FILE: '/api/check-file',
    CHECK_DATA: '/api/check-data',
    STATUSES: '/api/statuses',
    ACTIONS: '/api/actions'  
};

// Конфигурация порогов
const THRESHOLDS = {
    TIME: {
        MILLISECONDS: 9999, // до 10 секунд показываем в мс
        SECONDS: 59999,     // до 1 минуты показываем в секундах
        MINUTES: 3599999    // до 1 часа показываем в минутах
    },
    SIZE: {
        KB: 9999,           // до 10 КБ показываем в КБ
        MB: 10485759,       // до 10 МБ показываем в МБ
        // больше 10 МБ показываем в ГБ
    }
};

// Utility functions
function showNotification(message, isError = true) {
    const notification = document.getElementById('notification');
    notification.textContent = message;
    notification.className = `notification ${isError ? '' : 'success'}`;
    notification.style.display = 'block';
    
    setTimeout(() => {
        notification.style.display = 'none';
    }, 3000);
}

async function loadActions() {
    try {
        const response = await fetch(API_ENDPOINTS.ACTIONS);
        const data = await response.json();

        if (data.success && data.actions) {
            const actionSelect = document.getElementById('action');
            if (actionSelect) {
                // Сохраняем текущее значение
                const currentValue = actionSelect.value;

                // Очищаем опции кроме первой
                actionSelect.innerHTML = '<option value="">Все</option>';

                // Добавляем actions (только значения)
                data.actions.forEach(action => {
                    const option = document.createElement('option');
                    option.value = action;
                    option.textContent = action;
                    actionSelect.appendChild(option);
                });

                // Восстанавливаем выбранное значение
                if (currentValue && Array.from(actionSelect.options).some(opt => opt.value === currentValue)) {
                    actionSelect.value = currentValue;
                }
            }
        }
    } catch (error) {
        console.error('Ошибка загрузки actions:', error);
    }
}

async function loadStatuses() {
    try {
        const response = await fetch(API_ENDPOINTS.STATUSES);
        const data = await response.json();

        if (data.success && data.statuses) {
            const statusSelect = document.getElementById('status');
            data.statuses.sort((a, b) => a - b);
            if (statusSelect) {
                // Сохраняем текущее значение
                const currentValue = statusSelect.value;

                // Очищаем опции кроме первой
                statusSelect.innerHTML = '<option value="">Все</option>';

                // Добавляем статусы
                data.statuses.forEach(status => {
                    const option = document.createElement('option');
                    option.value = status;
                    option.textContent = status;
                    statusSelect.appendChild(option);
                });

                // Восстанавливаем выбранное значение
                if (currentValue && Array.from(statusSelect.options).some(opt => opt.value === currentValue)) {
                    statusSelect.value = currentValue;
                }
            }
        }
    } catch (error) {
        console.error('Ошибка загрузки статусов:', error);
    }
}

function getFilters() {
    return {
        dateFrom: document.getElementById('dateFrom').value,
        dateTo: document.getElementById('dateTo').value,
        clientIp: document.getElementById('clientIp').value,
        username: document.getElementById('username').value,
        status: document.getElementById('status').value,
        action: document.getElementById('action').value,
        search: document.getElementById('search').value
    };
}


function calculateRemainingTime(status) {
    if (!status.processed || !status.total || !startTime || status.processed === 0) {
        return 'расчет...';
    }
    
    const elapsed = (Date.now() - startTime) / 1000; // в секундах
    const processed = status.processed;
    const total = status.total;
    
    if (processed >= total || total <= 0) {
        return 'завершено';
    }
    
    // Расчет скорости (строк в секунду)
    const speed = processed / elapsed;
    
    if (speed === 0 || speed < 0.001) {
        return 'расчет...';
    }
    
    // Расчет оставшегося времени
    const remaining = total - processed;
    
    // Защита от деления на ноль и отрицательных значений
    if (remaining <= 0) {
        return 'завершено';
    }
    
    const secondsRemaining = remaining / speed;
    
    // Ограничиваем максимальное время (24 часа)
    const maxSeconds = 24 * 3600;
    const actualSeconds = Math.min(secondsRemaining, maxSeconds);
    
    // Форматирование времени
    if (actualSeconds < 60) {
        return `осталось: ~${Math.round(actualSeconds)} сек`;
    } else if (actualSeconds < 3600) {
        const minutes = Math.floor(actualSeconds / 60);
        const seconds = Math.round(actualSeconds % 60);
        return `осталось: ~${minutes} мин ${seconds} сек`;
    } else {
        const hours = Math.floor(actualSeconds / 3600);
        const minutes = Math.round((actualSeconds % 3600) / 60);
        return `осталось: ~${hours} ч ${minutes} мин`;
    }
}

function formatDateTimeLocal(date) {
    return date.toISOString().slice(0, 16);
}

function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

function updateProgress(status, progress, stage, isError = false, details = '') {
    const progressBar = document.getElementById('parsingProgressBar');
    const progressText = document.getElementById('parsingProgressText');
    const statusElement = document.getElementById('parsingStatus');
    const detailsElement = document.getElementById('parsingDetails');
    const stageElement = document.getElementById('parsingStage');
    const startButton = document.getElementById('startParsingBtn');
    
    if (progressBar) progressBar.style.width = progress + '%';
    if (progressText) progressText.textContent = `Прогресс: ${Math.round(progress)}%`;
    if (statusElement) {
        statusElement.textContent = status;
        statusElement.style.color = isError ? '#dc3545' : 
                                  progress >= 100 ? '#28a745' : 'var(--accent)';
    }
    
    // Добавляем детали если есть
    if (details && detailsElement) {
        detailsElement.textContent = details;
        detailsElement.style.display = 'block';
    }
    
    // Обновляем этап
    if (stageElement && stage > 0) {
        stageElement.textContent = getStageDescription(stage);
        stageElement.style.display = 'block';
    }
    
    // Обновляем кнопку
    if (startButton) {
        if (isError) {
            startButton.textContent = '🚀 Начать парсинг';
            startButton.disabled = false;
        } else {
            startButton.textContent = getStageButtonText(stage);
        }
    }
    
    console.log(`📊 [Этап ${stage}] ${status} - ${progress}%`);
}

function getStageDescription(stage) {
    const stages = {
        1: 'Подготовка файла',
        2: 'Загрузка на сервер',
        3: 'Проверка формата',
        4: 'Парсинг данных'
    };
    return stages[stage] || `Этап ${stage}`;
}

function getStageButtonText(stage) {
    const stageTexts = {
        1: '📁 Подготовка...',
        2: '⬆️ Загрузка...',
        3: '🔍 Проверка...',
        4: '⚙️ Парсинг...',
    };
    return stageTexts[stage] || '🚀 Начать парсинг';
}

// Data loading and display
async function loadData(page = 1) {
    // Защита от дублирования запросов
    if (isRequestInProgress) {
        showNotification('Уже выполняется другой запрос. Дождитесь завершения.', true);
        return;
    }
    
    const filters = getFilters();
    const loading = document.getElementById('loading');
    const table = document.getElementById('logsTable');
    const pagination = document.getElementById('pagination');
    
    // Определяем, пустые ли фильтры
    const isEmptyFilters = Object.values(filters).every(value => 
        value === '' || value === null || value === undefined
    );
    
    try {
        // Начало запроса
        isRequestInProgress = true;
        activeRequestType = 'loadData';
        requestStartTime = Date.now();
        

        showRequestStatus('Загрузка данных...', true);
        
        // Блокируем все кнопки
        disableAllButtons();
        
        // Создаем контроллер отмены
        const abortController = createAbortController();
        
        loading.style.display = 'block';
        if (table) table.style.display = 'none';
        if (pagination) pagination.style.display = 'none';
        
        const params = new URLSearchParams({
            page: page,
            size: itemsPerPage,
            ...filters
        });
        
        const response = await fetch(`${API_ENDPOINTS.LOGS}?${params}`, {
            signal: abortController.signal
        });
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        
        if (data.success) {
            allLogs = data.logs;
            displayLogs(data.logs);
            updateStats(data.stats);
            updatePagination(data.totalPages, page);
            createCharts(data.stats);
            
            // Успешное завершение с оптимизированным сообщением
            const recordCount = data.logs.length;

            finishRequestWithMessage(isEmptyFilters ? 'Кэшированная статистика загружена' : 'Данные загружены', true);
            
        } else {
            throw new Error('Ошибка загрузки данных: ' + (data.error || 'Неизвестная ошибка'));
        }
    } catch (error) {
        // Проверяем, была ли отмена
        if (error.name === 'AbortError') {
            console.log('Запрос отменен пользователем');
            return; // Не показываем ошибку при отмене
        }
        
        console.error('Ошибка:', error);
        showNotification('Произошла ошибка при загрузке данных');
        
        // Завершаем с ошибкой
        const requestTime = Date.now() - requestStartTime;
        showRequestStatus('Ошибка загрузки', false, requestTime);
        resetRequestState(); // Разблокирует кнопки
        
        // Через 2 секунды показываем "Готов"
        requestStatusTimeout = setTimeout(() => {
            showReadyStatus(requestTime);
        }, 2000);
    } finally {
        if (loading && !isRequestInProgress) {
            loading.style.display = 'none';
        }
    }
}

function displayLogs(logs) {
    const tbody = document.getElementById('logsBody');
    const fragment = document.createDocumentFragment();

    logs.forEach(log => {
        const row = document.createElement('tr');
        
        const statusClass = `status-${Math.floor(log.statusCode / 100) * 100}`;
        
        // Форматируем время ответа
        const responseTime = log.responseTime || 0;
        const formattedResponseTime = formatResponseTime(responseTime);
        
        // Форматируем размер ответа
        const responseSize = log.responseSize || 0;
        const formattedResponseSize = formatResponseSize(responseSize);
        
        // Обрезаем URL для отображения
        const displayUrl = log.url && log.url.length > 50 ? 
            log.url.substring(0, 50) + '...' : 
            (log.url || '');
        
        row.innerHTML = `
            <td>${new Date(log.time).toLocaleString()}</td>
            <td>${log.ip || ''}</td>
            <td>${log.username || ''}</td>
            <td class="${statusClass}">${log.statusCode || 0}</td>
            <td>${log.action || 'N/A'}</td>
            <td title="Точное значение: ${responseTime} мс">${formattedResponseTime}</td>
            <td title="Точное значение: ${responseSize} байт">${formattedResponseSize}</td>
            <td title="${log.url || ''}">${displayUrl}</td>
            <td>${log.domain || 'N/A'}</td>
        `;
        
        fragment.appendChild(row);
    });
    
    tbody.innerHTML = '';
    tbody.appendChild(fragment);
    document.getElementById('logsTable').style.display = 'table';
}

// Улучшенная функция для форматирования времени ответа
function formatResponseTime(ms) {
    if (!ms || ms <= 0) return '0 мс';
    
    // Более читаемая версия с порогами
    if (ms <= THRESHOLDS.TIME.MILLISECONDS) {
        // Меньше 10 секунд - миллисекунды
        return ms + ' мс';
    } else if (ms <= THRESHOLDS.TIME.SECONDS) {
        // Меньше 1 минуты - секунды
        const seconds = ms / 1000;
        return seconds < 10 ? 
            seconds.toFixed(1) + ' сек' : 
            Math.round(seconds) + ' сек';
    } else if (ms <= THRESHOLDS.TIME.MINUTES) {
        // Меньше 1 часа - минуты:секунды
        const seconds = ms / 1000;
        const minutes = Math.floor(seconds / 60);
        const remainingSeconds = Math.round(seconds % 60);
        return `${minutes}:${remainingSeconds.toString().padStart(2, '0')} мин`;
    } else {
        // Больше 1 часа - часы:минуты
        const hours = Math.floor(ms / 3600000);
        const minutes = Math.round((ms % 3600000) / 60000);
        return `${hours} ч ${minutes} мин`;
    }
}

// Улучшенная функция для форматирования размера ответа
function formatResponseSize(bytes) {
    if (!bytes || bytes <= 0) return '0 КБ';
    
    const kb = bytes / 1024;
    
    if (bytes <= THRESHOLDS.SIZE.KB) {
        // Меньше 10 КБ - КБ с двумя знаками
        return kb.toFixed(2) + ' КБ';
    } else if (bytes <= THRESHOLDS.SIZE.MB) {
        // Меньше 10 МБ - МБ
        const mb = kb / 1024;
        if (mb < 1) {
            // От 10 КБ до 1 МБ
            return mb.toFixed(2) + ' МБ';
        } else if (mb < 10) {
            // От 1 МБ до 10 МБ
            return mb.toFixed(1) + ' МБ';
        } else {
            // От 10 МБ до 10 ГБ
            return Math.round(mb) + ' МБ';
        }
    } else {
        // Больше 10 МБ - ГБ
        const gb = bytes / (1024 * 1024 * 1024);
        return gb.toFixed(2) + ' ГБ';
    }
}

function updateStats(stats) {
    document.getElementById('totalRequests').textContent = stats.total_requests.toLocaleString();
    document.getElementById('errorRequests').textContent = stats.error_requests.toLocaleString();
    
    // Форматируем среднее время ответа
    const avgTime = stats.avg_response_time || 0;
    document.getElementById('avgResponseTime').textContent = formatResponseTime(avgTime);
    
    document.getElementById('uniqueIps').textContent = stats.unique_ips.toLocaleString();
    
    // Форматируем общий трафик
    const totalTraffic = stats.total_traffic_mb || 0;
    document.getElementById('totalTraffic').textContent = formatTrafficMB(totalTraffic);
}

// Функция для форматирования трафика в МБ
function formatTrafficMB(mb) {
    if (!mb || mb <= 0) return '0 МБ';
    
    if (mb < 1024) {
        // Меньше 1 ГБ
        return mb < 100 ? 
            mb.toFixed(1) + ' МБ' : 
            Math.round(mb) + ' МБ';
    } else {
        // Больше 1 ГБ
        const gb = mb / 1024;
        return gb.toFixed(1) + ' ГБ';
    }
}

function updatePagination(total, current) {
    totalPages = total;
    currentPage = current;
    
    const pagination = document.getElementById('pagination');
    const pageInfo = document.getElementById('pageInfo');
    
    if (total > 1) {
        pageInfo.textContent = `Страница ${current} из ${total}`;
        pagination.style.display = 'flex';
        
        document.querySelector('.pagination button:first-child').disabled = current <= 1;
        document.querySelector('.pagination button:last-child').disabled = current >= total;
    } else {
        pagination.style.display = 'none';
    }
}

let selectedFilePath = "";

function browseFile() {
    // Для браузера - показать диалог выбора файла (но путь будет усечен)
    const input = document.createElement('input');
    input.type = 'file';
    input.accept = '.log,.txt';
    input.onchange = (e) => {
        const file = e.target.files[0];
        if (file) {
            // В браузере мы получим только имя файла, не полный путь
            document.getElementById('filePathInput').value = file.name;
            updateFileInfo(file.name, file.size);
            selectedFilePath = file.name; // Только имя файла
        }
    };
    input.click();
}

function updateFileInfo(fileName, fileSize) {
    const fileInfo = document.getElementById('fileInfo');
    if (fileSize) {
        const sizeMB = (fileSize / (1024 * 1024)).toFixed(2);
        fileInfo.textContent = `Файл: ${fileName} (${sizeMB} MB)`;
        fileInfo.className = 'file-info success';
    } else {
        fileInfo.textContent = `Укажите полный путь к файлу на сервере`;
        fileInfo.className = 'file-info';
    }
}

function getSelectedFilePath() {
    const input = document.getElementById('filePathInput');
    return input.value.trim();
}

// async function startParsing() {
//     const filePathInput = document.getElementById('filePathInput');
//     const filePath = filePathInput.value.trim();
//     const startButton = document.getElementById('startParsingBtn');
//     const originalText = startButton.textContent;
    
//     if (!filePath) {
//         showNotification('Введите путь к файлу логов');
//         return;
//     }
    
//     try {
//         startButton.disabled = true;
//         startButton.textContent = '⏳ Запуск парсинга...';
        
//         console.log('🚀 Начало парсинга файла:', filePath);
        
//         // Сбрасываем прогресс
//         resetParsingProgress();
//         startTime = Date.now();
        
//         // 1. Обновляем UI
//         updateProgressUI('Запуск парсинга...', 0, 'Проверка файла...');
        
//         console.log('📤 Отправка запроса на сервер...');
        
//         // 2. Отправляем путь к файлу на сервер
//         const parseResponse = await fetch(API_ENDPOINTS.START_PARSING, {
//             method: 'POST',
//             headers: {
//                 'Content-Type': 'application/json',
//             },
//             body: JSON.stringify({ filePath: filePath })
//         });
        
//         console.log('📨 Статус ответа:', parseResponse.status, parseResponse.statusText);
        
//         // Проверяем, есть ли контент в ответе
//         const responseText = await parseResponse.text();
//         console.log('📋 Текст ответа:', responseText);
        
//         if (!parseResponse.ok) {
//             throw new Error(`HTTP error! status: ${parseResponse.status}`);
//         }
        
//         // Пытаемся распарсить JSON
//         let parseData;
//         try {
//             parseData = JSON.parse(responseText);
//         } catch (jsonError) {
//             console.error('❌ Ошибка парсинга JSON:', jsonError);
//             console.error('Ответ сервера:', responseText);
//             throw new Error('Сервер вернул некорректный ответ. Проверьте консоль сервера.');
//         }
        
//         console.log('📋 Данные ответа парсинга:', parseData);
        
//         if (parseData.success) {
//             console.log('✅ Парсинг успешно запущен!');
//             showNotification('Парсинг запущен!', false);
            
//             // Сохраняем путь в localStorage
//             localStorage.setItem('lastLogFilePath', filePath);
            
//             // Запускаем отслеживание прогресса
//             startProgressPolling();
//         } else {
//             throw new Error(parseData.error || 'Ошибка запуска парсинга');
//         }
        
//     } catch (error) {
//         console.error('❌ Ошибка в процессе:', error);
//         showNotification('Ошибка: ' + error.message);
        
//         // Восстанавливаем кнопку
//         startButton.disabled = false;
//         startButton.textContent = '🚀 Начать парсинг';
        
//         // Сбрасываем прогресс
//         resetParsingProgress();
//     }
// }

function resetParsingProgress() {
    console.log('🔄 Сброс прогресса парсинга');
    
    // Останавливаем polling если он запущен
    stopProgressPolling();
    
    // Сбрасываем элементы UI
    const statusElement = document.getElementById('parsingStatus');
    const progressBar = document.getElementById('parsingProgressBar');
    const progressText = document.getElementById('parsingProgressText');
    const detailsElement = document.getElementById('parsingDetails');
    const stageElement = document.getElementById('parsingStage');
    const progressContainer = document.getElementById('parsingProgress'); // <-- ДОБАВЛЯЕМ ЗДЕСЬ
    
    if (statusElement) {
        statusElement.textContent = 'Готов к работе';
        statusElement.style.color = 'var(--text)';
    }
    
    if (progressBar) {
        progressBar.style.width = '0%';
    }
    
    if (progressText) {
        progressText.textContent = '0%'; // Проценты скрываются ниже
    }
    
    if (progressContainer) {
        progressContainer.style.display = 'none'; // Скрываем весь контейнер
    }
    
    if (detailsElement) {
        detailsElement.textContent = '';
        detailsElement.style.display = 'none';
    }
    
    if (stageElement) {
        stageElement.style.display = 'none';
    }
    
    // Сбрасываем временные переменные
    startTime = null;
    resetStagesState();
    
    console.log('✅ Прогресс парсинга сброшен');
}

function calculateStageTimeEstimates(totalLines) {
    // Эмпирические коэффициенты (миллисекунды на строку)
    const PARSING_SPEED = 0.05; // мс на строку (20 строк/мс)
    
    // Расчет времени для этапа парсинга
    const parsingTime = (totalLines * PARSING_SPEED) / 1000; // в секундах
    
    // Расчет общего времени
    const totalTime = parsingTime + 
        (AVG_STAGE_TIMES.FINALIZATION / 1000) + 
        (AVG_STAGE_TIMES.INDEXING / 1000) + 
        (AVG_STAGE_TIMES.STATISTICS / 1000);
    
    return {
        parsing: parsingTime,
        total: totalTime,
        stages: {
            COUNTING_LINES: 1, // секунда на подсчет
            PARSING: parsingTime,
            FINALIZATION: AVG_STAGE_TIMES.FINALIZATION / 1000,
            INDEXING: AVG_STAGE_TIMES.INDEXING / 1000,
            STATISTICS: AVG_STAGE_TIMES.STATISTICS / 1000
        }
    };
}


function startProgressPolling() {
    console.log('🔄 Запуск polling статуса парсинга');
    
    if (parsingInterval) {
        clearInterval(parsingInterval);
    }
    
    // Опрашиваем статус каждые 2 секунды
    parsingInterval = setInterval(async () => {
        try {
            console.log('📡 Запрос статуса парсинга...');
            const response = await fetch(API_ENDPOINTS.PARSING_STATUS);
            
            if (!response.ok) {
                console.error('❌ Ошибка запроса статуса:', response.status);
                return;
            }
            
            const statusText = await response.text();
            console.log('📋 Ответ статуса (текст):', statusText);
            
            if (!statusText) {
                console.warn('⚠️ Пустой ответ статуса');
                return;
            }
            
            const data = JSON.parse(statusText);
            console.log('📊 Данные статуса:', data);
            
            if (data.success) {
                updateParsingUI(data);
                
                // Если парсинг завершен
                if (!data.isParsing && data.progress >= 100) {
                    const totalTime = Date.now() - startTime;
                    showRequestStatus(`Парсинг завершен: `, false, totalTime);
                    console.log('✅ Парсинг завершен, останавливаем polling');
                    stopProgressPolling();
                    resetRequestState();
                }
            }
        } catch (error) {
            console.error('❌ Ошибка получения статуса:', error);
        }
    }, 2000); // Опрашиваем каждые 2 секунды
    
    // Первый запрос сразу
    setTimeout(() => {
        if (parsingInterval) {
            updateParsingStatus();
        }
    }, 500);
}

function stopProgressPolling() {
    if (parsingInterval) {
        clearInterval(parsingInterval);
        parsingInterval = null;
    }
    
    // При остановке polling сбрасываем состояние парсинга
    if (activeRequestType === 'parsing') {
        // Обновляем статус парсинга в UI
        const statusElement = document.getElementById('parsingStatus');
        if (statusElement && statusElement.textContent !== 'Парсинг отменен') {
            statusElement.textContent = 'Готов к работе';
            statusElement.style.color = 'var(--text)';
        }
    }
}

// ДОБАВЬТЕ обработчик ввода пути:
document.getElementById('filePathInput').addEventListener('input', function() {
    const filePath = this.value;
    const fileInfo = document.getElementById('fileInfo');
    const startButton = document.getElementById('startParsingBtn');

    if (startButton) {
        // При изменении пути - сбрасываем статус проверки
        startButton.setAttribute('data-file-valid', 'false');
        startButton.disabled = true;
        startButton.textContent = '🔍 Проверить файл';
    }
    
    if (filePath) {
        if (filePath.includes('/') || filePath.includes('\\')) {
            fileInfo.textContent = `Путь указан. Для парсинга нажмите "Начать парсинг"`;
            fileInfo.className = 'file-info';
            // НЕ разблокируем кнопку здесь
        } else {
            fileInfo.textContent = `⚠️ Укажите полный путь (например: D:/logs/access.log)`;
            fileInfo.className = 'file-info error';
        }
    } else {
        fileInfo.textContent = `Укажите полный путь к файлу на сервере`;
        fileInfo.className = 'file-info';
        // Блокируем кнопку если поле пустое
        document.getElementById('startParsingBtn').disabled = true;
    }
});

async function validateFilePath() {
    const filePathInput = document.getElementById('filePathInput');
    const filePath = filePathInput.value.trim();
    const startButton = document.getElementById('startParsingBtn');
    
    if (!filePath) {
        showNotification('Введите путь к файлу логов');
        filePathInput.style.borderColor = '#dc3545';
        startButton.disabled = true;
        startButton.setAttribute('data-file-valid', 'false');
        return false;
    }
    
    // Простая валидация пути
    const validExtensions = /\.(log|txt)(\.\w+)?$/i; // Разрешает .log, .txt, .log.m1, .log.gz и т.д.
    if (!validExtensions.test(filePath)) {
        showNotification('Файл должен иметь расширение .log, .txt или .log.xxx');
        filePathInput.style.borderColor = '#dc3545';
        startButton.disabled = true;
        startButton.setAttribute('data-file-valid', 'false');
        return false;
    }

    try {
        // Показываем статус проверки
        startButton.disabled = true;
        startButton.textContent = '🔍 Проверка...';
        
        const response = await fetch('/api/check-file', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ filePath: filePath })
        });
        
        const data = await response.json();
        
        if (data.exists) {
            filePathInput.style.borderColor = '#28a745';
            startButton.disabled = false;
            startButton.textContent = '🚀 Начать парсинг';
            startButton.setAttribute('data-file-valid', 'true');
            showNotification('Файл найден и готов к парсингу', false);
            return true;
        } else {
            filePathInput.style.borderColor = '#dc3545';
            startButton.disabled = true;
            startButton.textContent = '🔍 Проверить файл';
            startButton.setAttribute('data-file-valid', 'false');
            showNotification('Файл не найден по указанному пути');
            return false;
        }
    } catch (error) {
        console.error('Ошибка проверки файла:', error);
        filePathInput.style.borderColor = '#dc3545';
        startButton.disabled = true;
        startButton.textContent = '🔍 Проверить файл';
        startButton.setAttribute('data-file-valid', 'false');
        showNotification('Ошибка при проверке файла');
        return false;
    }
}

async function checkFileExists(filePath) {
    try {
        const response = await fetch('/api/check-file', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ filePath: filePath })
        });
        
        const data = await response.json();
        return data.exists || false;
    } catch (error) {
        console.error('Ошибка проверки файла:', error);
        return false;
    }
}
async function startParsing() {
    const filePathInput = document.getElementById('filePathInput');
    const filePath = filePathInput.value.trim();
    const startButton = document.getElementById('startParsingBtn');
    const progressContainer = document.getElementById('parsingProgress');
    
    if (!filePath) {
        showNotification('Введите путь к файлу логов');
        return;
    }
    
    // Проверяем, был ли файл проверен
    const isValid = startButton.getAttribute('data-file-valid') === 'true';
    if (!isValid) {
        showNotification('Сначала проверьте файл через кнопку "Проверить файл"');
        return;
    }
    
    // Сбрасываем статус проверки (чтобы кнопка была неактивна после парсинга)
    startButton.setAttribute('data-file-valid', 'false');

    // Убедимся, что предыдущее состояние сброшено
    if (isRequestInProgress && activeRequestType === 'parsing') {
        console.log('⚠️ Предыдущий парсинг был отменен, сбрасываем состояние...');
        resetRequestState();
        resetParsingUI();
    }
    
    // Проверяем, не выполняется ли уже другой запрос
    if (isRequestInProgress) {
        showNotification('Уже выполняется другой запрос. Дождитесь завершения.', true);
        return;
    }
    
    // Проверяем файл перед началом
    const fileExists = await checkFileExists(filePath);
    if (!fileExists) {
        showNotification('Файл не найден. Проверьте путь и попробуйте снова.');
        return;
    }
    
    try {
        // Начинаем парсинг
        isRequestInProgress = true;
        activeRequestType = 'parsing';
        requestStartTime = Date.now();
        
        startButton.disabled = true;
        startButton.textContent = '⏳ Запуск парсинга...';
        
        // Блокируем все кнопки
        disableAllButtons();
        
        console.log('🚀 Начало парсинга файла:', filePath);
        
        // Сбрасываем прогресс и показываем контейнер
        resetParsingProgress();
        if (progressContainer) {
            progressContainer.style.display = 'block'; // Показываем контейнер прогресса
        }
        startTime = Date.now();
        
        // 1. Обновляем UI
        const statusElement = document.getElementById('parsingStatus');
        if (statusElement) {
            statusElement.textContent = '📊 Подсчет строк (0%)';
            statusElement.style.color = 'var(--accent)';
        }
        
        // 2. Отправляем путь к файлу на сервер
        const parseResponse = await fetch(API_ENDPOINTS.START_PARSING, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ filePath: filePath })
        });
        
        console.log('📨 Ответ от сервера:', parseResponse.status);
        
        if (!parseResponse.ok) {
            throw new Error(`HTTP error! status: ${parseResponse.status}`);
        }
        
        const parseData = await parseResponse.json();
        console.log('📋 Данные ответа парсинга:', parseData);
        
        if (parseData.success) {
            console.log('✅ Парсинг успешно запущен!');
            showNotification('Парсинг запущен!', false);
            
            // Запускаем отслеживание прогресса
            startProgressPolling();
            
            // Показываем статус парсинга
            showRequestStatus('Парсинг запущен...', true);
            
        } else {
            throw new Error(parseData.error || 'Ошибка запуска парсинга');
        }
        
    } catch (error) {
        console.error('❌ Ошибка в процессе:', error);
        showNotification('Ошибка: ' + error.message);
        
        // Разблокируем все кнопки при ошибке
        resetRequestState();
        
        // Скрываем контейнер прогресса при ошибке
        if (progressContainer) {
            progressContainer.style.display = 'none';
        }
        
        // При ошибке возвращаем статус кнопки
        startButton.setAttribute('data-file-valid', 'false');
        startButton.textContent = '🔍 Проверить файл';
        
        // Сбрасываем прогресс
        resetParsingProgress();
    }
}

function updateProgressUI(status, progress, details = '') {
    const statusElement = document.getElementById('parsingStatus');
    const progressBar = document.getElementById('parsingProgressBar');
    const progressText = document.getElementById('parsingProgressText');
    
    if (progressBar) progressBar.style.width = progress + '%';
    if (progressText) progressText.textContent = `Прогресс: ${Math.round(progress)}%`;
    if (statusElement) {
        statusElement.textContent = status;
    }
}

// Добавляем обработчик Enter для поля ввода
document.addEventListener('DOMContentLoaded', function() {
    const filePathInput = document.getElementById('filePathInput');
    if (filePathInput) {
        filePathInput.addEventListener('keypress', function(e) {
            if (e.key === 'Enter') {
                validateFilePath();
            }
        });
    }

    const startButton = document.getElementById('startParsingBtn');
    if (startButton) {
        startButton.disabled = true;
        startButton.setAttribute('data-file-valid', 'false');
    }
});

async function updateParsingStatus() {
    try {
        const response = await fetch(API_ENDPOINTS.PARSING_STATUS);
        const data = await response.json();
        
        if (data.success) {
            updateParsingUI(data);
            
            if (!data.isParsing && data.progress >= 100) {
                stopProgressPolling();
                loadData();
            }
        }
    } catch (error) {
        console.error('Error fetching parsing status:', error);
    }
}

function updateParsingUI(status) {
    const statusElement = document.getElementById('parsingStatus');
    const progressBar = document.getElementById('parsingProgressBar');
    const progressText = document.getElementById('parsingProgressText');
    const detailsElement = document.getElementById('parsingDetails');
    const progressContainer = document.getElementById('parsingProgress');
    
    if (status.isParsing) {
        // Показываем контейнер прогресса
        if (progressContainer) {
            progressContainer.style.display = 'block';
        }
        
        // Обновляем прогресс-бар и процент общего прогресса
        progressBar.style.width = status.progress + '%';
        progressText.textContent = `${Math.round(status.progress)}%`;
        
        // Обновляем parsingStatus (название этапа + % этапа)
        statusElement.textContent = `${status.stageName} (${Math.round(status.stageProgress)}%)`;
        statusElement.style.color = 'var(--accent)';
        
        // Обновляем parsingDetails (обработанные строки + общее время)
        if (detailsElement) {
            const processed = status.processed?.toLocaleString() || '0';
            const total = status.total?.toLocaleString() || '0';
            const remainingTime = status.remaining || '~ расчет времени';
            
            // Форматируем время в удобный вид
            const formattedTime = remainingTime.replace('осталось: ~', '');
            
            detailsElement.textContent = 
                `Обработано: ${processed}/${total} строк • ` +
                `${formattedTime}`;
            detailsElement.style.display = 'block';
        }
        
    } else {
        // Парсинг завершен или отменен
        if (status.progress >= 100) {
            statusElement.textContent = '✅ Парсинг завершен';
            statusElement.style.color = '#28a745';
            
            // Скрываем прогресс-бар и проценты при успешном завершении
            if (progressContainer) {
                progressContainer.style.display = 'none';
            }
            
            if (detailsElement) {
                const totalMs = startTime ? (Date.now() - startTime) : 0;
                detailsElement.textContent = `Время выполнения: ${formatRequestTime(totalMs)}`;
            }
            
        } else {
            // Парсинг отменен или прерван
            statusElement.textContent = status.status || 'Готов к работе';
            statusElement.style.color = status.status && status.status.includes('отменен') ? '#dc3545' : 'var(--text)';
            
            // Скрываем прогресс-бар и проценты
            if (progressContainer) {
                progressContainer.style.display = 'none';
            }
            
            if (detailsElement) detailsElement.style.display = 'none';
        }
    }
}

function formatRequestTimeShort(milliseconds) {
    if (!milliseconds || milliseconds <= 0) return '0 мс';
    
    if (milliseconds < 1000) {
        return `${Math.round(milliseconds)} мс`;
    } else if (milliseconds < 60000) {
        const seconds = milliseconds / 1000;
        return `${seconds < 10 ? seconds.toFixed(1) : Math.round(seconds)} сек`;
    } else if (milliseconds < 3600000) {
        const minutes = Math.floor(milliseconds / 60000);
        const seconds = Math.round((milliseconds % 60000) / 1000);
        return seconds > 0 ? `${minutes} мин ${seconds} сек` : `${minutes} мин`;
    } else {
        const hours = Math.floor(milliseconds / 3600000);
        const minutes = Math.round((milliseconds % 3600000) / 60000);
        return minutes > 0 ? `${hours} ч ${minutes} мин` : `${hours} ч`;
    }
}

// Новая функция для парсинга этапа из статуса
function parseStageFromStatus(statusText) {
    const stages = {
        'Быстрый подсчет строк': { stage: 'COUNTING_LINES', name: '📊 Подсчет строк' },
        'Подсчет строк': { stage: 'COUNTING_LINES', name: '📊 Подсчет строк' },
        'Загрузка данных': { stage: 'PARSING', name: '🚀 Парсинг' },
        'Парсинг и подготовка данных': { stage: 'PARSING', name: '🚀 Парсинг' },
        'Финализация таблицы': { stage: 'FINALIZATION', name: '🗃️ Финализация' },
        'Создание индексов': { stage: 'INDEXING', name: '📈 Индексация' },
        'Обновление статистики': { stage: 'STATISTICS', name: '📊 Статистика' },
        'Вычисление статистики': { stage: 'STATISTICS', name: '📊 Статистика' }
    };
    
    for (const [key, value] of Object.entries(stages)) {
        if (statusText.includes(key)) {
            return value;
        }
    }
    
    return { stage: 'PARSING', name: '🚀 Парсинг' };
}

// Новая функция расчета общего прогресса
function calculateTotalProgress(currentStage, stageProgressPercent) {
    let progress = 0;
    
    // Добавляем прогресс завершенных этапов
    for (const [stage, weight] of Object.entries(STAGE_WEIGHTS)) {
        if (stage === currentStage) {
            // Текущий этап - добавляем его прогресс
            progress += weight * (stageProgressPercent / 100);
            break;
        } else {
            // Завершенные этапы - добавляем полностью
            progress += weight;
        }
    }
    
    return Math.min(100, progress * 100);
}

function calculateRemainingTimeWithStages(status, currentStage, stageProgress, totalProgress) {
    if (!status.processed || !status.total || !startTime) {
        return '~ расчет времени';
    }
    
    // Если это этап подсчета строк - используем старый расчет
    if (currentStage === 'COUNTING_LINES') {
        const result = calculateRemainingTime(status);
        return result.replace('осталось: ~', '');
    }
    
    // Если это этап парсинга данных
    if (currentStage === 'PARSING') {
        const elapsed = (Date.now() - startTime) / 1000;
        const processed = status.processed;
        const total = status.total;
        
        if (processed === 0 || elapsed === 0) return '~ расчет времени';
        
        const speed = processed / elapsed;
        const remainingLines = total - processed;
        const secondsRemainingLines = remainingLines / speed;
        
        // Добавляем среднее время для оставшихся этапов
        const remainingStagesTime = 
            AVG_STAGE_TIMES.FINALIZATION + 
            AVG_STAGE_TIMES.INDEXING + 
            AVG_STAGE_TIMES.STATISTICS;
        
        const totalSecondsRemaining = secondsRemainingLines + (remainingStagesTime / 1000);
        return formatRemainingTimeShort(totalSecondsRemaining);
    }
    
    // Для других этапов используем общий прогресс
    const elapsed = (Date.now() - startTime) / 1000;
    if (totalProgress === 0) return '~ расчет времени';
    
    const totalProgressPercent = totalProgress / 100;
    const speed = totalProgressPercent / elapsed;
    
    const remainingProgress = 1 - totalProgressPercent;
    const secondsRemaining = remainingProgress / speed;
    
    return formatRemainingTimeShort(secondsRemaining);
}

function formatRemainingTimeShort(seconds) {
    if (seconds < 60) {
        return `~${Math.round(seconds)} сек`;
    } else if (seconds < 3600) {
        const minutes = Math.floor(seconds / 60);
        const secs = Math.round(seconds % 60);
        return secs > 0 ? `~${minutes} мин ${secs} сек` : `~${minutes} мин`;
    } else {
        const hours = Math.floor(seconds / 3600);
        const minutes = Math.round((seconds % 3600) / 60);
        return minutes > 0 ? `~${hours} ч ${minutes} мин` : `~${hours} ч`;
    }
}

// Новая функция сброса состояния этапов
function resetStagesState() {
    currentStage = null;
    stageStartTime = null;
    stageProgress = 0;
    totalProgress = 0;
    stageEstimates = {};
    
    // Обнуляем UI элементы
    const progressText = document.getElementById('parsingProgressText');
    if (progressText) {
        progressText.textContent = '0%';
    }
}


// Вспомогательная функция для текста кнопки по этапам
function createCharts(stats) {
    const statusCtx = document.getElementById('statusChart').getContext('2d');
    const timeCtx = document.getElementById('timeChart').getContext('2d');
    
    // Получаем цвета для текущей темы
    const isDark = document.body.classList.contains('dark');
    const textColor = isDark ? '#e6edf3' : '#213043';
    const subtitleColor = isDark ? '#93a1b3' : '#666';
    const borderColor = isDark ? '#273242' : '#dbe3ec';
    const gridColor = isDark ? '#2a3547' : '#eef3f8';
    const tooltipBg = isDark ? '#ffffff' : 'rgba(0, 0, 0, 0.8)';
    const tooltipText = isDark ? '#000000' : '#ffffff';
    
    if (statusChart) statusChart.destroy();
    if (timeChart) timeChart.destroy();
    
    // 1. КРУГОВАЯ ДИАГРАММА для статусов с процентами
    const statusLabels = Object.keys(stats.status_distribution || {});
    const statusData = Object.values(stats.status_distribution || {});
    const totalStatuses = statusData.reduce((sum, value) => sum + value, 0);
    
    statusChart = new Chart(statusCtx, {
        type: 'pie',
        data: {
            labels: statusLabels,
            datasets: [{
                data: statusData,
                backgroundColor: [
                    '#4CAF50', '#2196F3', '#FF9800', '#F44336', '#9C27B0',
                    '#00BCD4', '#8BC34A', '#FFC107', '#795548', '#607D8B',
                    '#E91E63', '#3F51B5', '#009688', '#FF5722', '#673AB7'
                ],
                borderWidth: 2,
                borderColor: isDark ? '#1e1e1e' : '#ffffff',
                hoverBorderWidth: 3
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: {
                    display: true,
                    text: `Распределение HTTP статусов (Всего: ${totalStatuses.toLocaleString()})`,
                    color: textColor,
                    font: { 
                        size: 16,
                        weight: 'bold'
                    },
                    padding: 20
                },
                legend: {
                    position: 'right',
                    labels: { 
                        color: textColor, // ЦВЕТ ТЕКСТА ЛЕГЕНДЫ
                        font: { 
                            size: 12,
                            family: "'Segoe UI', Tahoma, Geneva, Verdana, sans-serif"
                        },
                        padding: 15,
                        usePointStyle: true,
                        pointStyle: 'circle',
                        // Важное исправление: функция generateLabels должна возвращать правильные цвета
                        generateLabels: function(chart) {
                            const data = chart.data;
                            if (data.labels.length && data.datasets.length) {
                                return data.labels.map((label, i) => {
                                    const value = data.datasets[0].data[i];
                                    const percentage = totalStatuses > 0 ? 
                                        ((value / totalStatuses) * 100).toFixed(1) : 0;
                                    
                                    return {
                                        text: `${label}: ${value.toLocaleString()} (${percentage}%)`,
                                        fillStyle: data.datasets[0].backgroundColor[i],
                                        strokeStyle: data.datasets[0].borderColor,
                                        lineWidth: data.datasets[0].borderWidth,
                                        hidden: false,
                                        index: i
                                    };
                                });
                            }
                            return [];
                        }
                    }
                },
                tooltip: {
                    backgroundColor: tooltipBg,
                    titleColor: tooltipText,
                    bodyColor: tooltipText,
                    borderColor: isDark ? 'rgba(0, 0, 0, 0.2)' : 'rgba(255, 255, 255, 0.2)',
                    borderWidth: 1,
                    padding: 12,
                    cornerRadius: 6,
                    displayColors: true,
                    callbacks: {
                        label: function(context) {
                            const label = context.label;
                            const value = context.raw || 0;
                            const percentage = totalStatuses > 0 ? 
                                ((value / totalStatuses) * 100).toFixed(1) : 0;
                            return `${label}: ${value.toLocaleString()} (${percentage}%)`;
                        }
                    }
                }
            },
            layout: {
                padding: {
                    left: 10,
                    right: 10,
                    top: 10,
                    bottom: 10
                }
            }
        }
    });
    
    // 2. ТОЧЕЧНАЯ ДИАГРАММА для времени суток
    timeChart = new Chart(timeCtx, {
        type: 'scatter',
        data: {
            datasets: [{
                label: '', // УБИРАЕМ МЕТКУ
                data: (stats.hourly_distribution || Array(24).fill(0)).map((count, hour) => ({
                    x: hour,
                    y: count
                })),
                backgroundColor: isDark ? 'rgba(43, 115, 196, 0.7)' : 'rgba(0, 123, 255, 0.7)',
                borderColor: isDark ? '#2b73c4' : '#007bff',
                borderWidth: 1,
                pointRadius: 6,
                pointHoverRadius: 10,
                showLine: true,
                lineTension: 0.3
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: {
                    display: true,
                    text: 'Запросы по времени суток',
                    color: textColor,
                    font: { 
                        size: 16,
                        weight: 'bold'
                    },
                    padding: 20
                },
                legend: { // УБИРАЕМ ЛЕГЕНДУ ПОЛНОСТЬЮ
                    display: false
                },
                tooltip: {
                    backgroundColor: tooltipBg,
                    titleColor: tooltipText,
                    bodyColor: tooltipText,
                    borderColor: isDark ? 'rgba(0, 0, 0, 0.2)' : 'rgba(255, 255, 255, 0.2)',
                    callbacks: {
                        label: function(context) {
                            const hour = context.parsed.x;
                            const count = context.parsed.y;
                            return `Время: ${hour}:00 - ${hour+1}:00\nЗапросов: ${count.toLocaleString()}`;
                        }
                    }
                }
            },
            scales: {
                x: {
                    type: 'linear',
                    position: 'bottom',
                    title: {
                        display: true,
                        text: 'Час дня',
                        color: textColor
                    },
                    min: 0,
                    max: 23,
                    ticks: {
                        stepSize: 1,
                        callback: function(value) {
                            return `${value}:00`;
                        },
                        color: textColor
                    },
                    grid: {
                        color: gridColor
                    }
                },
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Количество запросов',
                        color: textColor
                    },
                    ticks: {
                        precision: 0,
                        color: textColor
                    },
                    grid: {
                        color: gridColor
                    }
                }
            }
        }
    });
    
    // Обновляем функцию для кастомной легенды с правильными цветами
    updateChartLegend(statusChart, totalStatuses, textColor);
    enhanceTimeChart(timeChart, stats);
}

// Функция для создания кастомной легенды с правильным цветом текста
function updateChartLegend(chart, total, textColor) {
    if (!chart || !chart.options.plugins.legend) return;
    
    // Устанавливаем цвет текста легенды
    chart.options.plugins.legend.labels.color = textColor;
    
    // Обновляем функцию generateLabels для сохранения цветов
    chart.options.plugins.legend.labels.generateLabels = function(chart) {
        const data = chart.data;
        if (data.labels.length && data.datasets.length) {
            return data.labels.map((label, i) => {
                const value = data.datasets[0].data[i];
                const percentage = total > 0 ? ((value / total) * 100).toFixed(1) : 0;
                
                return {
                    text: `${label}: ${value.toLocaleString()} (${percentage}%)`,
                    fillStyle: data.datasets[0].backgroundColor[i],
                    strokeStyle: data.datasets[0].borderColor,
                    lineWidth: data.datasets[0].borderWidth,
                    hidden: false,
                    index: i,
                    fontColor: textColor // Явно указываем цвет шрифта
                };
            });
        }
        return [];
    };
    
    chart.update();
}

// Если хотите добавить общую информацию к точечной диаграмме
function enhanceTimeChart(chart, stats) {
    if (!chart || !stats) return;
    
    const isDark = document.body.classList.contains('dark');
    const subtitleColor = isDark ? '#93a1b3' : '#666';
    
    const hourlyData = stats.hourly_distribution || Array(24).fill(0);
    const totalRequests = hourlyData.reduce((a, b) => a + b, 0);
    
    // Находим час с максимальной активностью
    let maxHour = 0;
    let maxValue = 0;
    hourlyData.forEach((value, hour) => {
        if (value > maxValue) {
            maxValue = value;
            maxHour = hour;
        }
    });
    
    // Находим час с минимальной активностью (но не нулевой)
    let minHour = 0;
    let minValue = Infinity;
    hourlyData.forEach((value, hour) => {
        if (value > 0 && value < minValue) {
            minValue = value;
            minHour = hour;
        }
    });
    
    // Если все значения нулевые, устанавливаем минимальный час как первый
    if (minValue === Infinity) {
        minHour = 0;
        minValue = hourlyData[0] || 0;
    }
    
    // Обновляем или добавляем подзаголовок (БЕЗ СЛОВА "запросы")
    chart.options.plugins.subtitle = {
        display: true,
        text: `Всего: ${totalRequests.toLocaleString()} • Пик: ${maxHour}:00 (${maxValue.toLocaleString()}) • Мин: ${minHour}:00 (${minValue.toLocaleString()})`,
        color: subtitleColor,
        font: { 
            size: 12,
            weight: 'normal',
            family: "'Segoe UI', Tahoma, Geneva, Verdana, sans-serif"
        },
        padding: {
            top: 5,
            bottom: 15
        }
    };
    
    chart.update();
}

// Пагинация
function changePage(delta) {
    const next = currentPage + delta;
    if (next < 1 || next > totalPages) return;
    loadData(next);
}

// Экспорт
function exportData(format) {
    if (!Array.isArray(allLogs) || allLogs.length === 0) {
        showNotification('Нет данных для экспорта');
        return;
    }

    if (format === 'json') {
        const blob = new Blob([JSON.stringify(allLogs, null, 2)], { type: 'application/json;charset=utf-8' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `logs_page${currentPage}.json`;
        a.click();
        URL.revokeObjectURL(url);
        return;
    }

    // CSV
    const headers = ['time','ip','username','statusCode','action','responseTime','responseSize','url','domain'];
    const rows = allLogs.map(l => [
        new Date(l.time).toISOString(),
        l.ip || '',
        l.username || '',
        l.statusCode ?? '',
        l.action || '',
        l.responseTime ?? '',
        l.responseSize ?? '',
        (l.url || '').replace(/\n|\r|\t/g, ' '),
        l.domain || ''
    ]);

    const csv = [headers.join(','), ...rows.map(r => r.map(v => {
        const s = String(v ?? '');
        if (s.includes(',') || s.includes('"')) {
            return '"' + s.replace(/"/g, '""') + '"';
        }
        return s;
    }).join(','))].join('\n');

    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `logs_page${currentPage}.csv`;
    a.click();
    URL.revokeObjectURL(url);
}

// Топы
async function showTopUrls() {
    if (isRequestInProgress) {
        showNotification('Уже выполняется другой запрос. Дождитесь завершения.', true);
        return;
    }
    
    try {
        isRequestInProgress = true;
        activeRequestType = 'topUrls';
        requestStartTime = Date.now();
        
        // Получаем текущие фильтры
        const filters = getFiltersForTops();
        
        showRequestStatus('Загрузка топ URL...', true);
        disableAllButtons();
        
        const abortController = createAbortController();
        
        // Формируем URL с параметрами фильтров
        const params = new URLSearchParams({
            limit: 100,
            ...filters
        });
        
        const response = await fetch(`${API_ENDPOINTS.TOP_URLS}?${params}`, {
            signal: abortController.signal
        });
        
        const data = await response.json();
        
        if (data.success) {
            displayTopUrls(data.data);
            
            // Обновляем заголовок модального окна с информацией о фильтрах
            updateTopModalTitle('topUrlsModal', '🌐 Топ URL', filters);
            
            openModal('topUrlsModal');
            finishRequestWithMessage('Топ URL загружен', true);
        } else {
            throw new Error(data.error || 'Ошибка получения топ URL');
        }
    } catch (error) {
        if (error.name === 'AbortError') return;
        
        console.error('Ошибка загрузки топ URL:', error);
        showNotification('Не удалось загрузить топ URL: ' + error.message);
        
        const requestTime = Date.now() - requestStartTime;
        showRequestStatus('Ошибка загрузки', false, requestTime);
        resetRequestState();
        
        requestStatusTimeout = setTimeout(() => {
            showReadyStatus(requestTime);
        }, 2000);
    }
}

// Модифицированная функция showTopUsers
async function showTopUsers() {
    if (isRequestInProgress) {
        showNotification('Уже выполняется другой запрос. Дождитесь завершения.', true);
        return;
    }
    
    try {
        isRequestInProgress = true;
        activeRequestType = 'topUsers';
        requestStartTime = Date.now();
        
        // Получаем текущие фильтры
        const filters = getFiltersForTops();
        
        showRequestStatus('Загрузка топ пользователей...', true);
        disableAllButtons();
        
        const abortController = createAbortController();
        
        // Формируем URL с параметрами фильтров
        const params = new URLSearchParams({
            limit: 10,
            ...filters
        });
        
        const response = await fetch(`${API_ENDPOINTS.TOP_USERS}?${params}`, {
            signal: abortController.signal
        });
        
        const data = await response.json();
        
        if (data.success) {
            displayTopUsers(data.data);
            
            // Обновляем заголовок модального окна
            updateTopModalTitle('topUsersModal', '👤 Топ пользователей', filters);
            
            openModal('topUsersModal');
            finishRequestWithMessage('Топ пользователей загружен', true);
        } else {
            throw new Error(data.error || 'Ошибка получения топ пользователей');
        }
    } catch (error) {
        if (error.name === 'AbortError') return;
        
        console.error('Ошибка загрузки топ пользователей:', error);
        showNotification('Не удалось загрузить топ пользователей: ' + error.message);
        
        const requestTime = Date.now() - requestStartTime;
        showRequestStatus('Ошибка загрузки', false, requestTime);
        resetRequestState();
        
        requestStatusTimeout = setTimeout(() => {
            showReadyStatus(requestTime);
        }, 2000);
    }
}

function updateTopModalTitle(modalId, baseTitle, filters) {
    const modal = document.getElementById(modalId);
    if (!modal) return;
    
    const header = modal.querySelector('.modal-header h2');
    if (!header) return;
    
    // Проверяем, есть ли активные фильтры
    const hasActiveFilters = Object.values(filters).some(value => 
        value && value.toString().trim() !== ''
    );
    
    if (hasActiveFilters) {
        // Создаем строку с информацией о фильтрах
        const filterInfo = [];
        
        if (filters.dateFrom) filterInfo.push(`с ${filters.dateFrom}`);
        if (filters.dateTo) filterInfo.push(`по ${filters.dateTo}`);
        if (filters.ip) filterInfo.push(`IP: ${filters.ip}`);
        if (filters.username) filterInfo.push(`пользователь: ${filters.username}`);
        if (filters.status) filterInfo.push(`статус: ${filters.status}`);
        if (filters.action) filterInfo.push(`действие: ${filters.action}`);
        
        header.textContent = `${baseTitle} (фильтры: ${filterInfo.join(', ')})`;
    } else {
        header.textContent = baseTitle;
    }
}

function handleEscapeKey(event) {
    if (event.key === 'Escape' && isRequestInProgress) {
        cancelCurrentRequest();
    }
}

function handleBeforeUnload() {
    if (isRequestInProgress && currentAbortController) {
        // Отменяем все активные запросы при закрытии страницы
        currentAbortController.abort();
        console.log('Запросы отменены при закрытии страницы');
    }
}

// Функции для отображения данных в модальных окнах
function displayTopUrls(urls) {
    const tbody = document.getElementById('topUrlsBody');
    if (!tbody) return;
    
    tbody.innerHTML = '';
    
    urls.forEach((url, index) => {
        const row = document.createElement('tr');
        
        // Обрезаем длинные URL для отображения
        const displayUrl = url.url && url.url.length > 80 ? 
            url.url.substring(0, 80) + '...' : (url.url || 'N/A');
        
        // Форматируем время ответа если есть
        const avgResponseTime = url.avg_response_time ? 
            formatResponseTime(url.avg_response_time) : 'N/A';
        
        // Форматируем трафик если есть
        const totalTraffic = url.total_mb ? 
            formatTrafficMB(url.total_mb) : 'N/A';
        
        row.innerHTML = `
            <td>${index + 1}</td>
            <td class="url-cell" title="${url.url || ''}">${displayUrl}</td>
            <td class="domain-cell">${url.domain || 'N/A'}</td>
            <td><strong>${(url.count || 0).toLocaleString()}</strong></td>
            <td>${avgResponseTime}</td>
            <td>${totalTraffic}</td>
        `;
        
        tbody.appendChild(row);
    });
    
    // Обновить заголовки таблицы если нужно
    updateTopTableHeaders('topUrlsTable', ['#', 'URL', 'Домен', 'Запросы', 'Ср. время', 'Трафик']);
}

function displayTopUsers(users) {
    const tbody = document.getElementById('topUsersBody');
    if (!tbody) return;
    
    tbody.innerHTML = '';
    
    users.forEach((user, index) => {
        const row = document.createElement('tr');
        
        const firstSeen = user.first_seen ? 
            new Date(user.first_seen).toLocaleString() : 'N/A';
        const lastSeen = user.last_seen ? 
            new Date(user.last_seen).toLocaleString() : 'N/A';
        
        // Форматируем время ответа если есть
        const avgResponseTime = user.avg_response_time ? 
            formatResponseTime(user.avg_response_time) : 'N/A';
        
        // Форматируем трафик если есть
        const totalTraffic = user.total_mb ? 
            formatTrafficMB(user.total_mb) : 'N/A';
        
        row.innerHTML = `
            <td>${index + 1}</td>
            <td><strong>${user.username || 'N/A'}</strong></td>
            <td><strong>${(user.count || 0).toLocaleString()}</strong></td>
            <td>${user.ip || 'N/A'}</td>
            <td>${firstSeen}</td>
            <td>${lastSeen}</td>
            <td>${avgResponseTime}</td>
            <td>${totalTraffic}</td>
        `;
        
        tbody.appendChild(row);
    });
    
    // Обновить заголовки таблицы если нужно
    updateTopTableHeaders('topUsersTable', 
        ['#', 'Пользователь', 'Запросы', 'IP', 'Первый запрос', 'Последний запрос', 'Ср. время', 'Трафик']);
}

function updateTopTableHeaders(tableId, headers) {
    const table = document.getElementById(tableId);
    if (!table) return;
    
    const thead = table.querySelector('thead');
    if (!thead) return;
    
    // Очищаем текущие заголовки
    thead.innerHTML = '';
    
    // Создаем новую строку заголовков
    const headerRow = document.createElement('tr');
    headers.forEach(header => {
        const th = document.createElement('th');
        th.textContent = header;
        headerRow.appendChild(th);
    });
    
    thead.appendChild(headerRow);
}

function getFiltersForTops() {
    return {
        dateFrom: document.getElementById('dateFrom').value,
        dateTo: document.getElementById('dateTo').value,
        ip: document.getElementById('clientIp').value,
        username: document.getElementById('username').value,
        status: document.getElementById('status').value,
        action: document.getElementById('action').value
    };
}

// Функции управления модальными окнами
function openModal(modalId) {
    const modal = document.getElementById(modalId);
    if (modal) {
        modal.style.display = 'block';
        document.body.style.overflow = 'hidden'; // Блокируем скролл страницы
    }
}

function closeModal(modalId) {
    const modal = document.getElementById(modalId);
    if (modal) {
        modal.style.display = 'none';
        document.body.style.overflow = 'auto'; // Восстанавливаем скролл
    }
}

// Закрытие модального окна при клике вне его
window.onclick = function(event) {
    const modals = document.querySelectorAll('.modal');
    modals.forEach(modal => {
        if (event.target === modal) {
            modal.style.display = 'none';
            document.body.style.overflow = 'auto';
        }
    });
}

// Закрытие по Escape
document.addEventListener('keydown', function(event) {
    if (event.key === 'Escape') {
        const modals = document.querySelectorAll('.modal');
        modals.forEach(modal => {
            if (modal.style.display === 'block') {
                modal.style.display = 'none';
                document.body.style.overflow = 'auto';
            }
        });
    }
});

// Функция экспорта топ данных
function exportTopData(type) {
    let data, filename, headers, rows;
    
    // Получаем текущие фильтры для имени файла
    const filters = getFiltersForTops();
    const hasFilters = Object.values(filters).some(value => 
        value && value.toString().trim() !== ''
    );
    
    if (type === 'urls') {
        const rowsElements = document.querySelectorAll('#topUrlsBody tr');
        data = Array.from(rowsElements).map(row => ({
            rank: row.cells[0].textContent,
            url: row.cells[1].title || row.cells[1].textContent,
            domain: row.cells[2].textContent,
            count: row.cells[3].textContent,
            avg_response_time: row.cells[4].textContent,
            total_traffic: row.cells[5].textContent
        }));
        
        filename = hasFilters ? 
            `top_urls_filtered_${Date.now()}.csv` : 
            'top_urls.csv';
            
        headers = ['Ранг', 'URL', 'Домен', 'Количество запросов', 'Среднее время ответа', 'Общий трафик'];
    } else {
        const rowsElements = document.querySelectorAll('#topUsersBody tr');
        data = Array.from(rowsElements).map(row => ({
            rank: row.cells[0].textContent,
            username: row.cells[1].textContent,
            count: row.cells[2].textContent,
            ip: row.cells[3].textContent,
            first_seen: row.cells[4].textContent,
            last_seen: row.cells[5].textContent,
            avg_response_time: row.cells[6].textContent,
            total_traffic: row.cells[7].textContent
        }));
        
        filename = hasFilters ? 
            `top_users_filtered_${Date.now()}.csv` : 
            'top_users.csv';
            
        headers = ['Ранг', 'Пользователь', 'Количество запросов', 'IP', 'Первый запрос', 'Последний запрос', 'Среднее время ответа', 'Общий трафик'];
    }
    
    // ... существующий код формирования CSV ...
}

// Сортировка таблицы (простая по клику заголовка)
function setupSorting() {
    const headers = document.querySelectorAll('#logsTable thead th[data-sort]');
    headers.forEach(h => {
        h.addEventListener('click', () => {
            const col = h.getAttribute('data-sort');
            if (currentSort.column === col) {
                currentSort.direction = currentSort.direction === 'asc' ? 'desc' : 'asc';
            } else {
                currentSort.column = col;
                currentSort.direction = 'asc';
            }
            // Клиентская сортировка текущей страницы
            const dir = currentSort.direction === 'asc' ? 1 : -1;
            allLogs.sort((a, b) => {
                const va = a[col];
                const vb = b[col];
                if (va == null && vb == null) return 0;
                if (va == null) return -1 * dir;
                if (vb == null) return 1 * dir;
                if (typeof va === 'number' && typeof vb === 'number') return (va - vb) * dir;
                return String(va).localeCompare(String(vb)) * dir;
            });
            displayLogs(allLogs);
        });
    });
}
function formatTime(seconds) {
    if (seconds < 60) {
        return `${Math.round(seconds)} сек`;
    } else if (seconds < 3600) {
        const minutes = Math.floor(seconds / 60);
        const secs = Math.round(seconds % 60);
        return `${minutes} мин ${secs} сек`;
    } else {
        const hours = Math.floor(seconds / 3600);
        const minutes = Math.round((seconds % 3600) / 60);
        return `${hours} ч ${minutes} мин`;
    }
}
// Initialization
function initializeAppWithStatus() {
    applySavedTheme();
    setupSorting();
    loadStatuses();
    loadActions();

    // Добавляем обработчики событий
    document.addEventListener('keydown', handleEscapeKey);
    window.addEventListener('beforeunload', handleBeforeUnload);
    
    // Инициализируем кнопку отмены
    const cancelBtn = document.getElementById('cancelRequestBtn');
    if (cancelBtn) {
        cancelBtn.addEventListener('click', cancelCurrentRequest);
    }

    const progressContainer = document.getElementById('parsingProgress');
    if (progressContainer) {
        progressContainer.style.display = 'none';
    }

    const startButton = document.getElementById('startParsingBtn');
    if (startButton) {
        startButton.disabled = true;
        startButton.setAttribute('data-file-valid', 'false');
        startButton.textContent = '🔍 Проверить файл';
    }

        // Добавьте обработчик для Enter в поле ввода
    const filePathInput = document.getElementById('filePathInput');
    if (filePathInput) {
        filePathInput.addEventListener('keypress', function(e) {
            if (e.key === 'Enter') {
                validateFilePath();
            }
        });
    }
    
    // Инициализируем состояние кнопок
    enableAllButtons(); // Устанавливаем правильное начальное состояние
}

// Функция проверки начальных данных
async function checkInitialData() {
    try {
        const response = await fetch('/api/check-data');
        const data = await response.json();
        
        if (data.success) {
            if (data.hasData) {
                await loadData();
            } else {
                showNoDataMessage();
            }
        }
    } catch (error) {
        console.error('Ошибка проверки данных:', error);
        showNoDataMessage();
    }
}

function showNoDataMessage() {
    const tbody = document.getElementById('logsBody');
    const table = document.getElementById('logsTable');
    const stats = document.getElementById('stats');
    const charts = document.querySelector('.charts');
    
    // Скрываем таблицу, статистику и графики
    if (table) table.style.display = 'none';
    if (stats) stats.style.display = 'none';
    if (charts) charts.style.display = 'none';
    
    tbody.innerHTML = `
        <tr>
            <td colspan="9" style="text-align: center; padding: 40px;">
                <div style="max-width: 600px; margin: 0 auto;">
                    <h3>📊 Нет данных для отображения</h3>
                    <p style="margin: 15px 0; color: var(--muted);">
                        Чтобы начать анализ proxy-логов, укажите путь к файлу с логами и нажмите "Начать парсинг"
                    </p>
                    <div class="file-input-group" style="margin: 25px auto; max-width: 500px;">
                        <label for="filePathInput">Путь к файлу логов:</label>
                        <input type="text" 
                               id="filePathInput" 
                               class="file-path-input"
                               placeholder="C:\logs\proxy.log или /var/log/proxy.log"
                               value="D:/logs/notes.txt"
                               style="margin-bottom: 10px;">
                        <div style="display: flex; gap: 10px;">
                            <button onclick="validateFilePath()" class="btn-secondary" style="flex: 1;">
                                🔍 Проверить файл
                            </button>
                            <button onclick="startParsing()" id="startParsingBtn" class="btn-primary" disabled style="flex: 1;">
                                🚀 Начать парсинг
                            </button>
                        </div>
                    </div>
                    <p style="font-size: 13px; color: var(--muted); margin-top: 20px;">
                        Поддерживаются файлы формата .log и .txt
                    </p>
                </div>
            </td>
        </tr>
    `;
}

function clearFilters() {
    // Сброс полей фильтрации
    document.getElementById('dateFrom').value = '';
    document.getElementById('dateTo').value = '';
    document.getElementById('clientIp').value = '';
    document.getElementById('username').value = '';
    document.getElementById('status').value = '';
    document.getElementById('action').value = '';
    document.getElementById('search').value = '';
    
    // Перезагрузка данных
    // loadData();
    
    // Показать уведомление
    showNotification('Фильтры очищены', false);
    
    console.log('🧹 Фильтры очищены');
}

// Theme functionality
function applySavedTheme() {
    const saved = localStorage.getItem('theme') || 'light';
    if (saved === 'dark') {
        document.body.classList.add('dark');
    } else {
        document.body.classList.remove('dark');
    }
}

function toggleTheme() {
    const isDark = document.body.classList.contains('dark');
    document.body.classList.toggle('dark');
    const newIsDark = document.body.classList.contains('dark');
    
    localStorage.setItem('theme', newIsDark ? 'dark' : 'light');
    
    // Немедленно обновляем тему диаграмм
    setTimeout(() => {
        updateChartsTheme();
    }, 50); // Небольшая задержка для гарантии применения CSS классов
    
    showNotification(`Тема изменена на ${newIsDark ? 'тёмную' : 'светлую'}`, false);
}

function updateChartsTheme() {
    const isDark = document.body.classList.contains('dark');
    const textColor = isDark ? '#e6edf3' : '#213043'; // Светлая тема: темный текст (#213043)
    const subtitleColor = isDark ? '#93a1b3' : '#666';
    const gridColor = isDark ? '#2a3547' : '#eef3f8';
    const tooltipBg = isDark ? '#ffffff' : 'rgba(0, 0, 0, 0.8)';
    const tooltipText = isDark ? '#000000' : '#ffffff';
    const legendTextColor = textColor; // Цвет текста легенды должен совпадать с основным цветом текста
    
    console.log(`🔄 Обновление темы диаграмм: ${isDark ? 'темная' : 'светлая'}, цвет текста: ${textColor}`);
    
    // Обновляем круговую диаграмму (статусы)
    if (statusChart) {
        // Сохраняем данные перед обновлением
        const data = statusChart.data;
        const totalStatuses = data.datasets[0].data.reduce((sum, val) => sum + val, 0);
        
        // Обновляем заголовок
        statusChart.options.plugins.title.color = textColor;
        
        // Обновляем легенду - ПРАВИЛЬНО устанавливаем цвет текста
        statusChart.options.plugins.legend.labels.color = legendTextColor;
        
        // Также обновляем функцию generateLabels, чтобы она использовала правильный цвет
        statusChart.options.plugins.legend.labels.generateLabels = function(chart) {
            const data = chart.data;
            if (data.labels.length && data.datasets.length) {
                const total = data.datasets[0].data.reduce((sum, val) => sum + val, 0);
                
                return data.labels.map((label, i) => {
                    const value = data.datasets[0].data[i];
                    const percentage = total > 0 ? ((value / total) * 100).toFixed(1) : 0;
                    
                    return {
                        text: `${label}: ${value.toLocaleString()} (${percentage}%)`,
                        fillStyle: data.datasets[0].backgroundColor[i],
                        strokeStyle: data.datasets[0].borderColor,
                        lineWidth: data.datasets[0].borderWidth,
                        hidden: false,
                        index: i,
                        fontColor: legendTextColor // Явно указываем цвет шрифта
                    };
                });
            }
            return [];
        };
        
        // Обновляем тултипы
        statusChart.options.plugins.tooltip.backgroundColor = tooltipBg;
        statusChart.options.plugins.tooltip.titleColor = tooltipText;
        statusChart.options.plugins.tooltip.bodyColor = tooltipText;
        
        // Обновляем границу сегментов
        statusChart.data.datasets[0].borderColor = isDark ? '#1e1e1e' : '#ffffff';
        
        // Принудительное обновление диаграммы
        statusChart.update();
        console.log('✅ Круговая диаграмма обновлена');
    }
    
    // Обновляем точечную диаграмму (время суток)
    if (timeChart) {
        timeChart.options.plugins.title.color = textColor;
        
        if (timeChart.options.plugins.subtitle) {
            timeChart.options.plugins.subtitle.color = subtitleColor;
        }
        
        timeChart.options.plugins.tooltip.backgroundColor = tooltipBg;
        timeChart.options.plugins.tooltip.titleColor = tooltipText;
        timeChart.options.plugins.tooltip.bodyColor = tooltipText;
        timeChart.options.scales.x.title.color = textColor;
        timeChart.options.scales.x.ticks.color = textColor;
        timeChart.options.scales.x.grid.color = gridColor;
        timeChart.options.scales.y.title.color = textColor;
        timeChart.options.scales.y.ticks.color = textColor;
        timeChart.options.scales.y.grid.color = gridColor;
        timeChart.data.datasets[0].backgroundColor = isDark ? 'rgba(43, 115, 196, 0.7)' : 'rgba(0, 123, 255, 0.7)';
        timeChart.data.datasets[0].borderColor = isDark ? '#2b73c4' : '#007bff';
        
        timeChart.update();
        console.log('✅ Точечная диаграмма обновлена');
    }
}

function showRequestStatus(message, isLoading = false, requestTime = null) {
    const statusElement = document.getElementById('statusText');
    const cancelBtn = document.getElementById('cancelRequestBtn');
    const requestStatus = document.querySelector('.request-status');
    
    if (!statusElement) return;
    
    // Очищаем предыдущий таймаут
    if (requestStatusTimeout) {
        clearTimeout(requestStatusTimeout);
        requestStatusTimeout = null;
    }
    
    // Убираем время из основного текста, оно будет только в data-атрибуте
    statusElement.textContent = message;
    
    // Управляем спиннером через CSS класс
    if (isLoading) {
        statusElement.classList.add('loading');
        requestStatus.classList.add('loading');
        if (cancelBtn) cancelBtn.style.display = 'block';
    } else {
        statusElement.classList.remove('loading');
        requestStatus.classList.remove('loading');
        if (cancelBtn) cancelBtn.style.display = 'none';
    }
    
    // Сохраняем время выполнения в data-атрибут (для псевдоэлемента CSS)
    if (requestTime !== null) {
        const formattedTime = formatRequestTime(requestTime);
        statusElement.setAttribute('data-time', formattedTime);
    } else {
        statusElement.removeAttribute('data-time');
    }
}

// Функция для отображения статуса "Готов"  
function showReadyStatus(requestTime = null) {
    const statusElement = document.getElementById('statusText');
    const cancelBtn = document.getElementById('cancelRequestBtn');
    
    if (!statusElement) return;
    
    // Очищаем таймаут если есть
    if (requestStatusTimeout) {
        clearTimeout(requestStatusTimeout);
        requestStatusTimeout = null;
    }
    
    let statusText = 'Готов';
    
    if (requestTime !== null && requestTime > 0) {
        const formattedTime = formatRequestTimeShort(requestTime);
        statusText = `Время выполнения: `;
    }
    
    statusElement.textContent = statusText;
    statusElement.classList.remove('loading');
    
    // Скрываем кнопку отмены
    if (cancelBtn) cancelBtn.style.display = 'none';
    
    // Убираем класс loading с контейнера
    const requestStatus = document.querySelector('.request-status');
    if (requestStatus) requestStatus.classList.remove('loading');
}

// Форматирование времени запроса
function formatRequestTime(milliseconds) {
    // Защита от некорректных значений
    if (!milliseconds || milliseconds <= 0 || milliseconds > 24 * 3600 * 1000) {
        return '0 мс';
    }
    
    if (milliseconds < 1000) {
        return `${Math.round(milliseconds)} мс`;
    } else if (milliseconds < 60000) {
        const seconds = milliseconds / 1000;
        return `${seconds < 10 ? seconds.toFixed(1) : Math.round(seconds)} сек`;
    } else if (milliseconds < 3600000) {
        const minutes = Math.floor(milliseconds / 60000);
        const seconds = Math.round((milliseconds % 60000) / 1000);
        return `${minutes} мин ${seconds} сек`;
    } else {
        const hours = Math.floor(milliseconds / 3600000);
        const minutes = Math.round((milliseconds % 3600000) / 60000);
        return `${hours} ч ${minutes} мин`;
    }
}

// Функция для завершения запроса с сообщением
function finishRequestWithMessage(message, showReadyAfterDelay = true) {
    const requestTime = Date.now() - requestStartTime;
    
    // Защита от отрицательных или нереальных значений времени
    const safeRequestTime = Math.max(0, Math.min(requestTime, 24 * 3600 * 1000)); // Максимум 24 часа
    
    // Показываем сообщение БЕЗ времени в тексте
    showRequestStatus(message, false, safeRequestTime);
    
    // Сбрасываем состояние запроса (разблокирует кнопки)
    resetRequestState();
    
    // Через 2 секунды показываем "Готов" с временем выполнения
    if (showReadyAfterDelay) {
        requestStatusTimeout = setTimeout(() => {
            showReadyStatus(safeRequestTime);
        }, 2000);
    }
}

// Создание нового контроллера отмены
function createAbortController() {
    if (currentAbortController) {
        currentAbortController.abort();
    }
    currentAbortController = new AbortController();
    return currentAbortController;
}

// Отмена текущего запроса
function cancelCurrentRequest() {
    if (!isRequestInProgress) return;
    
    console.log('❌ Отмена текущего запроса');
    
    // Фиксируем время до отмены и вычисляем фактическое время выполнения
    const cancelTime = Date.now();
    const actualRequestTime = Math.max(0, cancelTime - requestStartTime);
    
    console.log(`Время выполнения до отмены: ${actualRequestTime} мс`);
    
    // Если это парсинг - отправляем запрос на отмену парсинга
    if (activeRequestType === 'parsing') {
        cancelParsing();
        return;
    }
    
    // Для обычных запросов - используем существующую логику
    if (currentAbortController) {
        currentAbortController.abort();
    }
    
    // Сбрасываем состояние ДО того как показываем статус
    resetRequestState();
    
    // Показываем статус отмены с корректным временем
    showRequestStatus('Запрос отменен', false, actualRequestTime);
    
    // Разблокируем все кнопки
    enableAllButtons();
    
    // Через 2 секунды показываем "Готов"
    requestStatusTimeout = setTimeout(() => {
        showReadyStatus(actualRequestTime);
    }, 2000);
    
    // Показываем уведомление
    showNotification('Запрос отменен', true);
}

async function cancelParsing() {
    try {
        console.log('🛑 Отправка запроса на отмену парсинга...');
        
        // Фиксируем время ДО отправки запроса
        const cancelStartTime = Date.now();
        
        // Вычисляем фактическое время выполнения парсинга
        const actualParsingTime = cancelStartTime - requestStartTime;
        console.log(`Фактическое время парсинга: ${actualParsingTime} мс`);
        
        const response = await fetch(API_ENDPOINTS.CANCEL_PARSING, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            signal: AbortSignal.timeout(5000) // Таймаут 5 секунд
        });
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        
        if (data.success) {
            console.log('✅ Парсинг отменен на сервере');
            
            // Останавливаем polling
            stopProgressPolling();
            
            // Сбрасываем UI парсинга
            resetParsingUI();
            
            // Сбрасываем состояние запроса (ВАЖНО!)
            resetRequestState();
            
            // Разблокируем все кнопки
            enableAllButtons();
            
            // Показываем статус с КОРРЕКТНЫМ временем выполнения парсинга
            showRequestStatus('Парсинг отменен', false, actualParsingTime);
            
            // Уведомление
            showNotification('Парсинг успешно отменен', true);
            
            // Через 2 секунды показываем "Готов"
            requestStatusTimeout = setTimeout(() => {
                showReadyStatus(actualParsingTime);
            }, 2000);
            
        } else {
            throw new Error(data.error || 'Не удалось отменить парсинг');
        }
        
    } catch (error) {
        console.error('❌ Ошибка отмены парсинга:', error);
        
        // Все равно показываем время выполнения
        const actualParsingTime = Date.now() - requestStartTime;
        showRequestStatus('Ошибка отмены', false, actualParsingTime);
        
        // Сбрасываем состояние
        resetRequestState();
        enableAllButtons();
        
        showNotification('Ошибка при отмене парсинга', true);
        
        // Через 2 секунды показываем "Готов"
        requestStatusTimeout = setTimeout(() => {
            showReadyStatus(actualParsingTime);
        }, 2000);
    }
}

function resetParsingUI() {
    const statusElement = document.getElementById('parsingStatus');
    const progressBar = document.getElementById('parsingProgressBar');
    const progressText = document.getElementById('parsingProgressText');
    const detailsElement = document.getElementById('parsingDetails');
    const stageElement = document.getElementById('parsingStage');
    const progressContainer = document.getElementById('parsingProgress'); // <-- ДОБАВЛЯЕМ ЗДЕСЬ
    
    if (statusElement) {
        statusElement.textContent = 'Парсинг отменен';
        statusElement.style.color = '#dc3545';
    }
    
    if (progressBar) {
        progressBar.style.width = '0%';
    }
    
    if (progressText) {
        progressText.textContent = '0%';
    }
    
    if (progressContainer) {
        progressContainer.style.display = 'none'; // Скрываем контейнер
    }
    
    if (detailsElement) {
        detailsElement.textContent = 'Операция прервана пользователем';
        detailsElement.style.display = 'block';
        
        // Через 3 секунды скрываем детали
        setTimeout(() => {
            detailsElement.style.display = 'none';
        }, 3000);
    }
    
    if (stageElement) {
        stageElement.style.display = 'none';
    }
    
    // Останавливаем polling
    if (parsingInterval) {
        clearInterval(parsingInterval);
        parsingInterval = null;
    }
    
    // Сбрасываем временные переменные
    if (startTime) {
        const actualTime = Date.now() - startTime;
        console.log(`Время выполнения парсинга: ${actualTime} мс`);
        startTime = null;
    }
}

// Сброс состояния запроса
function resetRequestState() {
    isRequestInProgress = false;
    activeRequestType = null;
    requestStartTime = null;
    
    if (currentAbortController) {
        currentAbortController.abort();
        currentAbortController = null;
    }
    
    // Разблокируем все кнопки
    enableAllButtons();
    
    // Скрываем кнопку отмены
    const cancelBtn = document.getElementById('cancelRequestBtn');
    if (cancelBtn) cancelBtn.style.display = 'none';
    
    // Убираем спиннер
    const statusElement = document.getElementById('statusText');
    const requestStatus = document.querySelector('.request-status');
    if (statusElement) statusElement.classList.remove('loading');
    if (requestStatus) requestStatus.classList.remove('loading');
    
    console.log('🔄 Состояние запроса сброшено');
}

function disableAllButtons() {
    // Кнопки фильтров
    const filterButtons = document.querySelectorAll('.filters button:not(.btn-cancel)');
    filterButtons.forEach(button => {
        button.disabled = true;
    });
    
    // Кнопки в панели действий
    const actionButtons = document.querySelectorAll('.actions-buttons button');
    actionButtons.forEach(button => {
        button.disabled = true;
    });
    
    // Кнопки парсинга
    const parsingButton = document.getElementById('startParsingBtn');
    if (parsingButton) {
        parsingButton.disabled = true;
    }
    
    // Кнопки в шапке (тема)
    const themeButton = document.querySelector('.header-theme-button button');
    if (themeButton) {
        themeButton.disabled = true;
    }
    
    // Кнопки пагинации
    const paginationButtons = document.querySelectorAll('.pagination button');
    paginationButtons.forEach(button => {
        button.disabled = true;
    });
    
    console.log('🔒 Все кнопки заблокированы');
}

// Разблокирует все кнопки после завершения запроса
function enableAllButtons() {
    // Кнопки фильтров
    const filterButtons = document.querySelectorAll('.filters button:not(.btn-cancel)');
    filterButtons.forEach(button => {
        button.disabled = false;
    });
    
    // Кнопки в панели действий
    const actionButtons = document.querySelectorAll('.actions-buttons button');
    actionButtons.forEach(button => {
        button.disabled = false;
    });

    
    // Кнопки в шапке (тема)
    const themeButton = document.querySelector('.header-theme-button button');
    if (themeButton) {
        themeButton.disabled = false;
    }
    
    // Кнопки пагинации (только если данные загружены)
    if (allLogs && allLogs.length > 0) {
        const paginationButtons = document.querySelectorAll('.pagination button');
        paginationButtons.forEach(button => {
            button.disabled = false;
        });
    }
    
    if (!parsingInterval) {
        const parsingButton = document.getElementById('startParsingBtn');
        const fileInput = document.getElementById('filePathInput');
        
        if (parsingButton && fileInput) {
            const isValid = parsingButton.getAttribute('data-file-valid') === 'true';
            parsingButton.disabled = !isValid;
            
            if (!isValid && fileInput.value.trim()) {
                parsingButton.textContent = '🔍 Проверить файл';
            } else if (isValid) {
                parsingButton.textContent = '🚀 Начать парсинг';
            }
        }
    }

    console.log('🔓 Все кнопки разблокированы');
}

// Make functions globally available for HTML onclick handlers
window.loadData = loadData;
window.clearFilters = clearFilters; // Добавить эту строку
window.changePage = changePage;
window.exportData = exportData;
window.showTopUrls = showTopUrls;
window.showTopUsers = showTopUsers;
window.toggleTheme = toggleTheme;
window.startParsing = startParsing;
window.resetParsingProgress = resetParsingProgress;

window.cancelCurrentRequest = cancelCurrentRequest;
window.initializeApp = initializeAppWithStatus;

// Event listeners
document.addEventListener('DOMContentLoaded', initializeApp);
window.addEventListener('beforeunload', stopProgressPolling);