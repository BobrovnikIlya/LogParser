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

// API endpoints
const API_ENDPOINTS = {
    LOGS: '/api/logs',
    TOP_URLS: '/api/top-urls',
    TOP_USERS: '/api/top-users',
    START_PARSING: '/api/start-file-parsing',
    PARSING_STATUS: '/api/parsing-status',
    CHECK_FILE: '/api/check-file',  // Добавляем новый endpoint
    CHECK_DATA: '/api/check-data'
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

function getFilters() {
    return {
        dateFrom: document.getElementById('dateFrom').value,
        dateTo: document.getElementById('dateTo').value,
        clientIp: document.getElementById('clientIp').value,
        username: document.getElementById('username').value,
        status: document.getElementById('status').value,
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
    
    if (processed >= total) {
        return 'завершено';
    }
    
    // Расчет скорости (строк в секунду)
    const speed = processed / elapsed;
    
    if (speed === 0) {
        return 'расчет...';
    }
    
    // Расчет оставшегося времени
    const remaining = total - processed;
    const secondsRemaining = remaining / speed;
    
    // Форматирование времени
    if (secondsRemaining < 60) {
        return `осталось: ~${Math.round(secondsRemaining)} сек`;
    } else if (secondsRemaining < 3600) {
        const minutes = Math.floor(secondsRemaining / 60);
        const seconds = Math.round(secondsRemaining % 60);
        return `осталось: ~${minutes} мин ${seconds} сек`;
    } else {
        const hours = Math.floor(secondsRemaining / 3600);
        const minutes = Math.round((secondsRemaining % 3600) / 60);
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
            
            // Успешное завершение
            const recordCount = data.logs.length;
            finishRequestWithMessage(`Загружено ${recordCount} записей`, true);
            
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
        const sizeKB = (log.responseSize / 1024).toFixed(2);
        const displayUrl = log.url.length > 50 ? log.url.substring(0, 50) + '...' : log.url;
        
        row.innerHTML = `
            <td>${new Date(log.time).toLocaleString()}</td>
            <td>${log.ip}</td>
            <td>${log.username || ''}</td>
            <td class="${statusClass}">${log.statusCode}</td>
            <td>${log.action || 'N/A'}</td>
            <td>${log.responseTime || 0}мс</td>
            <td>${sizeKB} КБ</td>
            <td title="${log.url}">${displayUrl}</td>
            <td>${log.domain || 'N/A'}</td>
        `;
        
        fragment.appendChild(row);
    });
    
    tbody.innerHTML = '';
    tbody.appendChild(fragment);
    document.getElementById('logsTable').style.display = 'table';
}

function updateStats(stats) {
    document.getElementById('totalRequests').textContent = stats.total_requests.toLocaleString();
    document.getElementById('errorRequests').textContent = stats.error_requests.toLocaleString();
    document.getElementById('avgResponseTime').textContent = stats.avg_response_time.toLocaleString();
    document.getElementById('uniqueIps').textContent = stats.unique_ips.toLocaleString();
    document.getElementById('totalTraffic').textContent = stats.total_traffic_mb.toLocaleString();
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

async function startParsing() {
    const filePathInput = document.getElementById('filePathInput');
    const filePath = filePathInput.value.trim();
    const startButton = document.getElementById('startParsingBtn');
    const originalText = startButton.textContent;
    
    if (!filePath) {
        showNotification('Введите путь к файлу логов');
        return;
    }
    
    try {
        startButton.disabled = true;
        startButton.textContent = '⏳ Запуск парсинга...';
        
        console.log('🚀 Начало парсинга файла:', filePath);
        
        // Сбрасываем прогресс
        resetParsingProgress();
        startTime = Date.now();
        
        // 1. Обновляем UI
        updateProgressUI('Запуск парсинга...', 0, 'Проверка файла...');
        
        console.log('📤 Отправка запроса на сервер...');
        
        // 2. Отправляем путь к файлу на сервер
        const parseResponse = await fetch(API_ENDPOINTS.START_PARSING, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ filePath: filePath })
        });
        
        console.log('📨 Статус ответа:', parseResponse.status, parseResponse.statusText);
        
        // Проверяем, есть ли контент в ответе
        const responseText = await parseResponse.text();
        console.log('📋 Текст ответа:', responseText);
        
        if (!parseResponse.ok) {
            throw new Error(`HTTP error! status: ${parseResponse.status}`);
        }
        
        // Пытаемся распарсить JSON
        let parseData;
        try {
            parseData = JSON.parse(responseText);
        } catch (jsonError) {
            console.error('❌ Ошибка парсинга JSON:', jsonError);
            console.error('Ответ сервера:', responseText);
            throw new Error('Сервер вернул некорректный ответ. Проверьте консоль сервера.');
        }
        
        console.log('📋 Данные ответа парсинга:', parseData);
        
        if (parseData.success) {
            console.log('✅ Парсинг успешно запущен!');
            showNotification('Парсинг запущен!', false);
            
            // Сохраняем путь в localStorage
            localStorage.setItem('lastLogFilePath', filePath);
            
            // Запускаем отслеживание прогресса
            startProgressPolling();
        } else {
            throw new Error(parseData.error || 'Ошибка запуска парсинга');
        }
        
    } catch (error) {
        console.error('❌ Ошибка в процессе:', error);
        showNotification('Ошибка: ' + error.message);
        
        // Восстанавливаем кнопку
        startButton.disabled = false;
        startButton.textContent = '🚀 Начать парсинг';
        
        // Сбрасываем прогресс
        resetParsingProgress();
    }
}

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
    
    if (statusElement) {
        statusElement.textContent = 'Готов к работе';
        statusElement.style.color = 'var(--text)';
    }
    
    if (progressBar) {
        progressBar.style.width = '0%';
    }
    
    if (progressText) {
        progressText.textContent = 'Прогресс: 0%';
    }
    
    if (detailsElement) {
        detailsElement.textContent = '';
        detailsElement.style.display = 'none';
    }
    
    if (stageElement) {
        stageElement.textContent = '';
        stageElement.style.display = 'none';
    }
    
    // Сбрасываем временные переменные
    startTime = null;
    
    console.log('✅ Прогресс парсинга сброшен');
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
                    console.log('✅ Парсинг завершен, останавливаем polling');
                    stopProgressPolling();
                    
                    // Загружаем данные
                    setTimeout(() => {
                        loadData();
                        showNotification('Парсинг завершен! Данные загружены.', false);
                    }, 1000);
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
    
    // Разблокируем кнопки после завершения парсинга
    if (activeRequestType === 'parsing') {
        resetRequestState();
    }
}

// ДОБАВЬТЕ обработчик ввода пути:
document.getElementById('filePathInput').addEventListener('input', function() {
    const filePath = this.value;
    const fileInfo = document.getElementById('fileInfo');
    
    if (filePath) {
        if (filePath.includes('/') || filePath.includes('\\')) {
            fileInfo.textContent = `Путь указан. Для парсинга нажмите "Начать парсинг"`;
            fileInfo.className = 'file-info';
            
            // Разблокируем кнопку парсинга
            document.getElementById('startParsingBtn').disabled = false;
        } else {
            fileInfo.textContent = `⚠️ Укажите полный путь (например: D:/logs/access.log)`;
            fileInfo.className = 'file-info error';
        }
    } else {
        fileInfo.textContent = `Укажите полный путь к файлу на сервере`;
        fileInfo.className = 'file-info';
        document.getElementById('startParsingBtn').disabled = true;
    }
});
function validateFilePath() {
    const filePathInput = document.getElementById('filePathInput');
    const filePath = filePathInput.value.trim();
    const startButton = document.getElementById('startParsingBtn');
    
    if (!filePath) {
        showNotification('Введите путь к файлу логов');
        filePathInput.style.borderColor = '#dc3545';
        startButton.disabled = true;
        return false;
    }
    
    // Простая валидация пути
    if (!filePath.endsWith('.log') && !filePath.endsWith('.txt')) {
        showNotification('Файл должен иметь расширение .log или .txt');
        filePathInput.style.borderColor = '#dc3545';
        startButton.disabled = true;
        return false;
    }
    
    // Проверка существования файла через API
    checkFileExists(filePath).then(exists => {
        if (exists) {
            filePathInput.style.borderColor = '#28a745';
            startButton.disabled = false;
            showNotification('Файл найден и готов к парсингу', false);
        } else {
            filePathInput.style.borderColor = '#dc3545';
            startButton.disabled = true;
            showNotification('Файл не найден по указанному пути');
        }
    });
    
    return true;
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
    const originalText = startButton.textContent;
    
    if (!filePath) {
        showNotification('Введите путь к файлу логов');
        return;
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
        
        // Сбрасываем прогресс
        resetParsingProgress();
        startTime = Date.now();
        
        // 1. Обновляем UI
        updateProgressUI('Запуск парсинга...', 0, 'Проверка файла...');
        
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
        
        // Восстанавливаем кнопку
        startButton.disabled = false;
        startButton.textContent = '🚀 Начать парсинг';
        
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
    const detailsElement = document.getElementById('parsingDetails') || document.createElement('div');
    const stageElement = document.getElementById('parsingStage') || document.createElement('div');
    const button = document.getElementById('startParsingBtn');
    
    if (status.isParsing) {
        // Рассчитываем прогресс
        const parsingProgress = Math.min(100, Math.max(0, status.progress || 0));
        
        let statusText = status.status || 'Парсинг...';
        if (status.fileName) {
            statusText = `Парсинг: ${status.fileName}`;
        }
        
        statusElement.textContent = statusText;
        statusElement.style.color = 'var(--accent)';
        
        progressBar.style.width = parsingProgress + '%';
        
        const processed = status.processed?.toLocaleString() || '0';
        const total = status.total?.toLocaleString() || '0';
        const progressPercent = status.progress?.toFixed(1) || '0';
        
        progressText.textContent = `Прогресс: ${progressPercent}%`;
        
        // Обновляем детальную информацию
        if (detailsElement && detailsElement.textContent !== undefined) {
            detailsElement.textContent = `Обработано: ${processed}/${total} строк • ${calculateRemainingTime(status)}`;
            detailsElement.style.display = 'block';
        }
        
        // Обновляем этап
        if (stageElement && stageElement.textContent !== undefined) {
            stageElement.textContent = `Этап парсинга`;
            stageElement.style.display = 'block';
        }
        
        button.disabled = true;
        button.textContent = '⏳ Парсинг выполняется...';
        
    } else {
        if (status.progress >= 100) {
            statusElement.textContent = '✅ Парсинг завершен';
            statusElement.style.color = '#28a745';
            progressBar.style.width = '100%';
            progressText.textContent = 'Прогресс: 100%';
            
            if (detailsElement && detailsElement.textContent !== undefined) {
                const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);
                detailsElement.textContent = `Обработано: ${status.processed?.toLocaleString() || '0'} строк • Общее время: ${totalTime} сек`;
            }
            
            // Разблокируем кнопку после завершения
            setTimeout(() => {
                button.disabled = false;
                button.textContent = '🚀 Начать парсинг';
                if (stageElement) stageElement.style.display = 'none';
            }, 2000);
        } else {
            statusElement.textContent = status.status || 'Готов к работе';
            statusElement.style.color = 'var(--text)';
            button.disabled = false;
            button.textContent = '🚀 Начать парсинг';
            if (detailsElement) detailsElement.style.display = 'none';
            if (stageElement) stageElement.style.display = 'none';
        }
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
        showRequestStatus('Загрузка топ URL...', true);

        disableAllButtons();
        
        const abortController = createAbortController();
        
        const response = await fetch(API_ENDPOINTS.TOP_URLS, {
            signal: abortController.signal
        });
        
        const data = await response.json();
        
        if (data.success) {
            displayTopUrls(data.data);
            openModal('topUrlsModal');
            finishRequestWithMessage('Топ 100 URL загружен', true);
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
        showRequestStatus('Загрузка топ пользователей...', true);

        disableAllButtons();
        
        const abortController = createAbortController();
        
        const response = await fetch(API_ENDPOINTS.TOP_USERS, {
            signal: abortController.signal
        });
        
        const data = await response.json();
        
        if (data.success) {
            displayTopUsers(data.data);
            openModal('topUsersModal');
            finishRequestWithMessage('Топ 10 пользователей загружен', true);
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
        const displayUrl = url.url.length > 80 ? 
            url.url.substring(0, 80) + '...' : url.url;
        
        row.innerHTML = `
            <td>${index + 1}</td>
            <td class="url-cell" title="${url.url}">${displayUrl}</td>
            <td class="domain-cell">${url.domain || 'N/A'}</td>
            <td><strong>${url.count.toLocaleString()}</strong></td>
        `;
        
        tbody.appendChild(row);
    });
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
        
        row.innerHTML = `
            <td>${index + 1}</td>
            <td><strong>${user.username}</strong></td>
            <td><strong>${user.count.toLocaleString()}</strong></td>
            <td>${user.unique_ips ? user.unique_ips.toLocaleString() : 'N/A'}</td>
            <td>${firstSeen}</td>
            <td>${lastSeen}</td>
        `;
        
        tbody.appendChild(row);
    });
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
    
    if (type === 'urls') {
        const rowsElements = document.querySelectorAll('#topUrlsBody tr');
        data = Array.from(rowsElements).map(row => ({
            rank: row.cells[0].textContent,
            url: row.cells[1].title || row.cells[1].textContent,
            domain: row.cells[2].textContent,
            count: row.cells[3].textContent
        }));
        filename = 'top_urls.csv';
        headers = ['Ранг', 'URL', 'Домен', 'Количество запросов'];
    } else {
        const rowsElements = document.querySelectorAll('#topUsersBody tr');
        data = Array.from(rowsElements).map(row => ({
            rank: row.cells[0].textContent,
            username: row.cells[1].textContent,
            count: row.cells[2].textContent,
            uniqueIps: row.cells[3].textContent,
            firstSeen: row.cells[4].textContent,
            lastSeen: row.cells[5].textContent
        }));
        filename = 'top_users.csv';
        headers = ['Ранг', 'Пользователь', 'Количество запросов', 'Уникальных IP', 'Первый запрос', 'Последний запрос'];
    }
    
    // Формируем CSV
    const csvRows = [
        headers.join(','),
        ...data.map(row => Object.values(row).map(value => {
            const stringValue = String(value);
            return stringValue.includes(',') ? `"${stringValue}"` : stringValue;
        }).join(','))
    ];
    
    const csv = csvRows.join('\n');
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = filename;
    link.click();
    URL.revokeObjectURL(url);
    
    showNotification(`Топ ${type === 'urls' ? 'URL' : 'пользователей'} экспортирован в CSV`, false);
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
    
    // Добавляем обработчики событий
    document.addEventListener('keydown', handleEscapeKey);
    window.addEventListener('beforeunload', handleBeforeUnload);
    
    // Инициализируем кнопку отмены
    const cancelBtn = document.getElementById('cancelRequestBtn');
    if (cancelBtn) {
        cancelBtn.addEventListener('click', cancelCurrentRequest);
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
    if (!statusElement) return;
    
    let statusText = 'Время выполнения:';
    
    if (requestTime !== null) {
        const formattedTime = formatRequestTime(requestTime);
        statusElement.setAttribute('data-time', formattedTime);
        statusText = 'Время выполнения:'; // Всегда показываем этот текст
    } else {
        statusElement.removeAttribute('data-time');
        statusText = 'Время выполнения:'; // Даже без времени показываем текст
    }
    
    statusElement.textContent = statusText;
    statusElement.classList.remove('loading');
    
    // Скрываем кнопку отмены
    const cancelBtn = document.getElementById('cancelRequestBtn');
    if (cancelBtn) cancelBtn.style.display = 'none';
    
    // Убираем класс loading с контейнера
    const requestStatus = document.querySelector('.request-status');
    if (requestStatus) requestStatus.classList.remove('loading');
}

// Форматирование времени запроса
function formatRequestTime(milliseconds) {
    if (milliseconds < 1000) {
        return `${Math.round(milliseconds)} мс`;
    } else if (milliseconds < 60000) {
        return `${(milliseconds / 1000).toFixed(1)} сек`;
    } else {
        const minutes = Math.floor(milliseconds / 60000);
        const seconds = Math.round((milliseconds % 60000) / 1000);
        return `${minutes}:${seconds.toString().padStart(2, '0')} мин`;
    }
}

// Функция для завершения запроса с сообщением
function finishRequestWithMessage(message, showReadyAfterDelay = true) {
    const requestTime = Date.now() - requestStartTime;
    
    // Показываем сообщение БЕЗ времени в тексте
    showRequestStatus(message, false, requestTime);
    
    // Сбрасываем состояние запроса (разблокирует кнопки)
    resetRequestState();
    
    // Через 2 секунды показываем "Готов" с временем выполнения
    if (showReadyAfterDelay) {
        requestStatusTimeout = setTimeout(() => {
            showReadyStatus(requestTime);
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
    if (!isRequestInProgress || !currentAbortController) return;
    
    console.log('❌ Отмена текущего запроса');
    
    // Прерываем запрос
    currentAbortController.abort();
    
    // Показываем статус отмены
    const requestTime = Date.now() - requestStartTime;
    showRequestStatus('Запрос отменен', false, requestTime);
    
    // Сбрасываем состояние
    resetRequestState();
    
    // Разблокируем все кнопки
    enableAllButtons();
    
    // Через 2 секунды показываем "Готов"
    requestStatusTimeout = setTimeout(() => {
        showReadyStatus(requestTime);
    }, 2000);
    
    // Показываем уведомление
    showNotification('Запрос отменен', true);
}

// Сброс состояния запроса
function resetRequestState() {
    isRequestInProgress = false;
    activeRequestType = null;
    requestStartTime = null;
    
    if (currentAbortController) {
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
    
    // Кнопки парсинга (только если парсинг не выполняется)
    if (!parsingInterval) {
        const parsingButton = document.getElementById('startParsingBtn');
        if (parsingButton) {
            const filePath = document.getElementById('filePathInput').value.trim();
            parsingButton.disabled = !filePath;
        }
    }
    
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