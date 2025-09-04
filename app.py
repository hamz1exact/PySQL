from flask import Flask, request, jsonify, render_template_string
from flask_cors import CORS
import json
import traceback
import re

# Import your existing modules - UPDATED FOR NEW DATABASE STRUCTURE

from engine import Lexer, Parser, db_manager
from executor import execute

app = Flask(__name__)
CORS(app)

# Initialize db_manager.active_dbmanager - NEW

def get_current_database():
    return db_manager.active_db
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SQL IDE Pro</title>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.12/codemirror.min.css">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.12/theme/material-darker.min.css">
    <style>
    document.addEventListener("DOMContentLoaded", () => {
    refreshTables();
    });
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'JetBrains Mono', 'Cascadia Code', 'Fira Code', 'Monaco', 'Consolas', monospace;
            background: #1e1e1e;
            color: #d4d4d4;
            height: 100vh;
            overflow: hidden;
        }
        
        .ide-container {
            display: flex;
            flex-direction: column;
            height: 100vh;
            background: #1e1e1e;
        }
        
        .title-bar {
            background: #2d2d30;
            border-bottom: 1px solid #3e3e42;
            padding: 8px 16px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            height: 35px;
        }
        
        .title-bar h1 {
            font-size: 14px;
            font-weight: 600;
            color: #cccccc;
            display: flex;
            align-items: center;
            gap: 8px;
        }
        
        .title-bar-buttons {
            display: flex;
            gap: 8px;
        }
        
        .btn {
            padding: 4px 12px;
            border: 1px solid #464647;
            border-radius: 3px;
            cursor: pointer;
            font-weight: 500;
            font-size: 12px;
            transition: all 0.2s ease;
            background: #3c3c3c;
            color: #cccccc;
            font-family: inherit;
        }
        
        .btn:hover {
            background: #4a4a4a;
            border-color: #007acc;
        }
        
        .btn-primary {
            background: #0e639c;
            border-color: #007acc;
            color: white;
        }
        
        .btn-primary:hover {
            background: #1177bb;
        }
        
        .btn-danger {
            background: #a1260d;
            border-color: #f85149;
            color: white;
        }
        
        .btn-danger:hover {
            background: #c5391a;
        }
        
        .main-layout {
            display: flex;
            flex: 1;
            overflow: hidden;
        }
        
        .sidebar {
            width: 250px;
            background: #252526;
            border-right: 1px solid #3e3e42;
            display: flex;
            flex-direction: column;
            transition: width 0.3s ease;
        }
        
        .sidebar.collapsed {
            width: 0px;
            overflow: hidden;
        }
        
        .sidebar-header {
            padding: 12px 16px;
            border-bottom: 1px solid #3e3e42;
            display: flex;
            align-items: center;
            justify-content: space-between;
        }
        
        .sidebar-title {
            font-size: 13px;
            font-weight: 600;
            color: #cccccc;
        }
        
        .sidebar-content {
            flex: 1;
            overflow-y: auto;
            padding: 8px 0;
        }
        
        .table-item {
            padding: 8px 16px;
            cursor: pointer;
            border-bottom: 1px solid #2d2d30;
            transition: background 0.2s;
        }
        
        .table-item:hover {
            background: #2a2d2e;
        }
        
        .table-name {
            font-weight: 600;
            color: #4ec9b0;
            font-size: 13px;
        }
        
        .table-info {
            font-size: 11px;
            color: #858585;
            margin-top: 2px;
        }
        /* Make table headers resizable */
        .table-result th {
            position: relative;
            user-select: none;
            cursor: col-resize;
        }

        /* The draggable handle on the right side of each th */
        .table-result th .resize-handle {
            position: absolute;
            right: 0;
            top: 0;
            width: 5px;
            height: 100%;
            cursor: col-resize;
            z-index: 1;
}

        
        .content-area {
            flex: 1;
            display: flex;
            flex-direction: column;
            background: #1e1e1e;
        }
        
        .toolbar {
            background: #2d2d30;
            border-bottom: 1px solid #3e3e42;
            padding: 8px 16px;
            display: flex;
            align-items: center;
            gap: 8px;
            height: 40px;
        }
        
        .editor-panel {
            background: #1e1e1e;
            border-bottom: 1px solid #3e3e42;
            display: flex;
            flex-direction: column;
            transition: height 0.3s ease;
            min-height: 200px;
        }
        
        .editor-header {
            background: #2d2d30;
            border-bottom: 1px solid #3e3e42;
            padding: 8px 16px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            height: 35px;
        }
        
        .editor-title {
            font-size: 13px;
            color: #cccccc;
        }
        
        .editor-container {
            flex: 1;
            min-height: 150px;
        }
        
        .CodeMirror {
            height: 100% !important;
            font-family: 'JetBrains Mono', 'Cascadia Code', 'Fira Code', 'Monaco', 'Consolas', monospace !important;
            font-size: 14px !important;
        }
        
        .resizer {
            height: 4px;
            background: #2d2d30;
            cursor: row-resize;
            border-top: 1px solid #3e3e42;
            border-bottom: 1px solid #3e3e42;
            position: relative;
        }
        
        .resizer:hover {
            background: #007acc;
        }
        
        .results-panel {
            flex: 1;
            display: flex;
            flex-direction: column;
            background: #1e1e1e;
            min-height: 200px;
        }
        
        .results-header {
            background: #2d2d30;
            border-bottom: 1px solid #3e3e42;
            padding: 8px 16px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            height: 35px;
        }
        
        .results-title {
            font-size: 13px;
            color: #cccccc;
        }
        
        .results-content {
            flex: 1;
            overflow: auto;
            background: #1e1e1e;
        }
        
        .status-bar {
            background: #007acc;
            color: white;
            padding: 4px 16px;
            font-size: 12px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            height: 25px;
        }
        
        .table-container {
            margin: 16px;
            border: 1px solid #3e3e42;
            border-radius: 4px;
            overflow: hidden;
            background: #252526;
        }
        
        /* Make custom-sql keywords blue */
        .cm-s-material-darker .cm-keyword {
            color: #18AEFF !important; /* Blue */
        }
        .cm-s-material-darker .cm-number {
            color: #F9A03F !important;
        }
        .cm-s-material-darker .cm-variable,
        .cm-s-material-darker .cm-type {
        color: #ffffff !important; /* White text */
}

        /* Optional: types can be green */
        .cm-s-material-darker .cm-type {
            color: #E6E69D !important;
        }

        .cm-s-material-darker .cm-string {
            color: #ce9178 !important;
        }

        .data-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 13px;
            font-family: 'JetBrains Mono', monospace;
        }
        
        .data-table th {
            background: #2d2d30;
            color: #4ec9b0;
            padding: 8px 12px;
            text-align: left;
            border-bottom: 1px solid #3e3e42;
            border-right: 1px solid #3e3e42;
            font-weight: 600;
            font-size: 12px;
            position: sticky;
            top: 0;
            min-width: 80px;
            cursor: grab;
            user-select: none;
            resize: horizontal;
            overflow: hidden;
        }
        .data-table th:active {
            cursor: grabbing;
            }

        .data-table th.dragging {
            opacity: 0.5;
            background: #007acc;
        }

        .data-table th.drop-target {
            background: #4ec9b0;
        }
        
        .data-table td {
            padding: 6px 12px;
            border-bottom: 1px solid #2d2d30;
            border-right: 1px solid #2d2d30;
            color: #d4d4d4;
            word-break: break-word;
        }
        
        .data-table tr:hover {
            background: #2a2d2e;
        }
        
        .data-table tr:nth-child(even) {
            background: rgba(255, 255, 255, 0.02);
        }
        
        .null-value {
            color: #608b4e;
            font-style: italic;
        }
        
        .number-value {
            color: #b5cea8;
            text-align: right;
        }
        
        .string-value {
            color: #ce9178;
        }
        
        .message {
            margin: 16px;
            padding: 12px 16px;
            border-radius: 4px;
            font-size: 13px;
        }
        
        .message-success {
            background: rgba(106, 153, 85, 0.1);
            border: 1px solid #6a9955;
            color: #6a9955;
        }
        
        .message-error {
            background: rgba(244, 71, 71, 0.1);
            border: 1px solid #f44747;
            color: #f44747;
        }
        
        .message-info {
            background: rgba(0, 122, 204, 0.1);
            border: 1px solid #007acc;
            color: #007acc;
        }
        
        .empty-state {
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            height: 200px;
            color: #858585;
            font-size: 13px;
        }
        
        .loading-spinner {
            width: 20px;
            height: 20px;
            border: 2px solid #3e3e42;
            border-top: 2px solid #007acc;
            border-radius: 50%;
            animation: spin 1s linear infinite;
            display: inline-block;
            margin-right: 8px;
        }
        
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
        
        .query-info {
            font-size: 11px;
            color: #858585;
            margin: 8px 16px;
            padding: 8px 12px;
            background: #2d2d30;
            border-radius: 3px;
        }
        
        ::-webkit-scrollbar {
            width: 12px;
            height: 12px;
        }
        
        ::-webkit-scrollbar-track {
            background: #2d2d30;
        }
        
        ::-webkit-scrollbar-thumb {
            background: #464647;
            border-radius: 6px;
        }
        
        ::-webkit-scrollbar-thumb:hover {
            background: #5a5a5c;
        }
    </style>
</head>
<body>
    <div class="ide-container">
        <div class="title-bar">
            <h1>
                <span>🗄️</span>
                SQL IDE Pro
            </h1>
            <div class="title-bar-buttons">
                <button class="btn" onclick="toggleSidebar()">Toggle Tables</button>
                <button class="btn btn-danger" onclick="clearAll()">Clear All</button>
            </div>
        </div>
        
        <div class="main-layout">
            <div class="sidebar" id="sidebar">
                <div class="sidebar-header">
                    <span class="sidebar-title">Tables</span>
                    <button class="btn" onclick="refreshTables()">↻</button>
                </div>
                <div class="sidebar-content" id="tables-list">
                    <div class="empty-state">
                        <span>Loading tables...</span>
                    </div>
                </div>
            </div>
            
            <div class="content-area">
                <div class="toolbar">
                    <button class="btn btn-primary" onclick="executeQuery()" title="Ctrl+Enter">
                        ▶ Execute
                    </button>
                    <button class="btn" onclick="executeAllQueries()" title="Ctrl+Shift+Enter">
                        ▶▶ Execute All
                    </button>
                    <button class="btn" onclick="clearEditor()">Clear Editor</button>
                </div>
                
                <div class="editor-panel" id="editor-panel">
                    <div class="editor-header">
                        <span class="editor-title">SQL Editor</span>
                    </div>
                    <div class="editor-container">
                        <textarea id="editor"></textarea>
                    </div>
                </div>
                
                <div class="resizer" id="resizer"></div>
                
                <div class="results-panel" id="results-panel">
                    <div class="results-header">
                        <span class="results-title">Query Results</span>
                    </div>
                    <div class="results-content" id="results-content">
                        <div class="empty-state">
                            <span>📊</span>
                            <p>Execute a query to see results here</p>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        
        <div class="status-bar">
            <span id="status-text">Ready</span>
            <span id="status-info">SQL IDE Pro v1.0</span>
        </div>
    </div>

    <script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.12/codemirror.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.12/mode/sql/sql.min.js"></script>
    
    <script>
        let editor;
        let currentResults = [];
        
        window.addEventListener('load', () => {
            // Define custom SQL keywords for your engine
            const customKeywords = {
                'USE': true, 'SET': true, 'SWITCH': true,
                'SELECT': true, 'FROM': true, 'WHERE': true, 'INSERT': true, 'UPDATE': true, 'DELETE': true,
                'CREATE': true, 'DROP': true, 'ALTER': true, 'TABLE': true, 'DATABASE': true,
                'AND': true, 'OR': true, 'NOT': true, 'IN': true, 'BETWEEN': true, 'LIKE': true,
                'ORDER': true, 'BY': true, 'GROUP': true, 'HAVING': true, 'LIMIT': true, 'USE': true
            };
            
            const customTypes = {
                'INT': true, 'STR': true, 'PLAINSTR': true,
                'VARCHAR': true, 'CHAR': true, 'TEXT': true, 'DATE': true, 'DATETIME': true,
                'DECIMAL': true, 'FLOAT': true, 'DOUBLE': true, 'BOOLEAN': true, 'BOOL': true,
                "AUTO_INT":true
            };
            
            // Create custom SQL mode
            CodeMirror.defineMode("custom-sql", function() {
                return {
                    token: function(stream, state) {
                        if (stream.eatSpace()) return null;
                        
                        // Comments
                        if (stream.match(/--.*$/)) {
                            return "comment";
                        }
                        if (stream.match(/\/\*[\s\S]*?\*\//)) {
                            return "comment";
                        }
                        
                        // Strings
                        if (stream.match(/'[^']*'/)) {
                            return "string";
                        }
                        if (stream.match(/"[^"]*"/)) {
                            return "string";
                        }
                        
                        // Numbers
                        if (stream.match(/\d+(\.\d+)?/)) {
                            return "number";
                        }
                        
                        // Keywords and types
                        const word = stream.match(/\w+/);
                        if (word) {
                            const upperWord = word[0].toUpperCase();
                            if (customKeywords[upperWord]) {
                                return "keyword"; // Blue color
                            }
                            if (customTypes[upperWord]) {
                                return "type"; // Different color for types
                            }
                            return "variable";
                        }
                        
                        stream.next();
                        return null;
                    }
                };
            });
            
            editor = CodeMirror.fromTextArea(document.getElementById('editor'), {
                mode: 'custom-sql',
                theme: 'material-darker',
                lineNumbers: true,
                lineWrapping: true,
                indentWithTabs: true,
                smartIndent: true,
                autofocus: true,
                extraKeys: {
                    "Ctrl-Enter": executeQuery,
                    "Ctrl-Shift-Enter": executeAllQueries,
                    "F5": executeQuery
                }
            });
            
            refreshTables();
            setupResizer();
        });
        
        function setupResizer() {
            const resizer = document.getElementById('resizer');
            const editorPanel = document.getElementById('editor-panel');
            const resultsPanel = document.getElementById('results-panel');
            let isResizing = false;
            
            resizer.addEventListener('mousedown', (e) => {
                isResizing = true;
                document.addEventListener('mousemove', handleResize);
                document.addEventListener('mouseup', () => {
                    isResizing = false;
                    document.removeEventListener('mousemove', handleResize);
                });
            });
            
            function handleResize(e) {
                if (!isResizing) return;
                
                const container = document.querySelector('.content-area');
                const containerRect = container.getBoundingClientRect();
                const mouseY = e.clientY - containerRect.top;
                const toolbarHeight = 40;
                const minHeight = 150;
                
                const editorHeight = Math.max(minHeight, mouseY - toolbarHeight);
                const totalHeight = containerRect.height - toolbarHeight - 4;
                const resultsHeight = Math.max(minHeight, totalHeight - editorHeight);
                
                editorPanel.style.height = `${editorHeight}px`;
                resultsPanel.style.height = `${resultsHeight}px`;
                
                if (editor) {
                    setTimeout(() => editor.refresh(), 0);
                }
            }
        }
        
        function toggleSidebar() {
            const sidebar = document.getElementById('sidebar');
            sidebar.classList.toggle('collapsed');
        }
        
        async function executeQuery() {
            let query;
            const selection = editor.getSelection();
            
            if (selection) {
                query = selection;
            } else {
                query = getCurrentQuery();
            }
            
            if (!query.trim()) {
                showMessage('No query to execute', 'error');
                return;
            }
            
            await runQuery(query.trim());
        }
        
        async function executeAllQueries() {
            const allText = editor.getValue();
            const queries = splitQueries(allText);
            
            if (queries.length === 0) {
                showMessage('No queries found', 'error');
                return;
            }
            
            updateStatus(`Executing ${queries.length} queries...`);
            
            for (let i = 0; i < queries.length; i++) {
                const query = queries[i].trim();
                if (query) {
                    updateStatus(`Executing query ${i + 1}/${queries.length}...`);
                    await runQuery(query, i === queries.length - 1);
                    await new Promise(resolve => setTimeout(resolve, 100));
                }
            }
        }
        
        function getCurrentQuery() {
            const cursor = editor.getCursor();
            const allText = editor.getValue();
            const lines = allText.split('\\n');
            
            let startLine = cursor.line;
            let endLine = cursor.line;
            
            // Find start of current query
            for (let i = startLine; i >= 0; i--) {
                if (lines[i].trim().endsWith(';')) {
                    startLine = i + 1;
                    break;
                }
                if (i === 0) startLine = 0;
            }
            
            // Find end of current query
            for (let i = endLine; i < lines.length; i++) {
                if (lines[i].trim().endsWith(';')) {
                    endLine = i;
                    break;
                }
                if (i === lines.length - 1) endLine = i;
            }
            
            return lines.slice(startLine, endLine + 1).join('\\n');
        }
        
        function splitQueries(text) {
            // First, remove comments properly
            let cleanedText = text;
            
            // Remove single-line comments (-- comment)
            cleanedText = cleanedText.replace(/--.*$/gm, '');
            
            // Remove multi-line comments (/* comment */)
            cleanedText = cleanedText.replace(/\/\*[\s\S]*?\*\//g, '');
            
            // Split by semicolons and filter out empty queries
            const queries = cleanedText.split(';')
                .map(q => q.trim())
                .filter(q => q.length > 0);
            
            // Add semicolon back to each query for parser
            return queries.map(q => q.endsWith(';') ? q : q + ';');
        }
        
        async function runQuery(query, isLast = true) {
            if (!query.trim()) return;

            updateStatus('Executing query...');

            try {
                const response = await fetch('/api/execute', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ query })
                });
                const result = await response.json();

                if (isLast) {
                    displayResults(result, query);
                }

                if (result.success) {
                    updateStatus(result.message || 'Query executed successfully');

                    // ✅ Automatically refresh tables if the query affects DB structure
                    const queryType = query.trim().split(' ')[0].toUpperCase();
                    if (['USE', 'CREATE', 'DROP', 'ALTER'].includes(queryType)) {
                        refreshTables();
                    }

                } else {
                    updateStatus(`Error: ${result.error}`);
                    if (isLast) {
                        showMessage(result.error, 'error');
                    }
                }

            } catch (error) {
                updateStatus(`Network error: ${error.message}`);
                if (isLast) {
                    showMessage(`Network error: ${error.message}`, 'error');
                }
            }
        }
        
        function displayResults(result, query) {
            const content = document.getElementById('results-content');
            
            if (!result.success) {
                content.innerHTML = `<div class="message message-error">${escapeHtml(result.error)}</div>`;
                return;
            }
            
            if (result.data && result.data.length > 0) {
                currentResults = result.data;
                const tableHtml = createDataTable(result.data, result.columns);

                const queryInfo = `<div class="query-info">
                    <strong>${result.data.length}</strong> rows returned • 
                    Query executed in <strong>${result.execution_time || 'N/A'}</strong>
                </div>`;
                
                content.innerHTML = tableHtml + queryInfo;
            } else if (result.message) {
                content.innerHTML = `<div class="message message-success">${escapeHtml(result.message)}</div>`;
            } else {
                content.innerHTML = `<div class="message message-info">Query executed successfully</div>`;
            }
        }
        function buildTable(columns, rows) {
    const table = document.createElement('table');
    table.className = 'table-result';
    
    // Table header
    const thead = document.createElement('thead');
    const tr = document.createElement('tr');
    columns.forEach(col => {
        const th = document.createElement('th');
        th.textContent = col;

        // Add resize handle
        const handle = document.createElement('div');
        handle.className = 'resize-handle';
        th.appendChild(handle);

        tr.appendChild(th);
    });
    thead.appendChild(tr);
    table.appendChild(thead);

    // Table body
    const tbody = document.createElement('tbody');
    rows.forEach(row => {
        const tr = document.createElement('tr');
        columns.forEach(col => {
            const td = document.createElement('td');
            td.textContent = row[col];
            tr.appendChild(td);
        });
        tbody.appendChild(tr);
    });
    table.appendChild(tbody);

    enableColumnResize(table); // attach resize logic
    return table;
}
function enableColumnResize(table) {
    const ths = table.querySelectorAll('th');
    ths.forEach(th => {
        const handle = th.querySelector('.resize-handle');
        let startX, startWidth;

        handle.addEventListener('mousedown', (e) => {
            startX = e.pageX;
            startWidth = th.offsetWidth;

            function onMouseMove(e) {
                const newWidth = startWidth + (e.pageX - startX);
                th.style.width = newWidth + 'px';
            }

            function onMouseUp() {
                document.removeEventListener('mousemove', onMouseMove);
                document.removeEventListener('mouseup', onMouseUp);
            }

            document.addEventListener('mousemove', onMouseMove);
            document.addEventListener('mouseup', onMouseUp);
        });
    });
}

        function createDataTable(data, columns) {
    if (!data || data.length === 0) {
        return '<div class="empty-state"><p>No data to display</p></div>';
    }

    // Use provided column order
    const headerRow = columns.map((col, index) =>
        `<th data-column="${col}" data-index="${index}" 
            style="position: relative; min-width: 80px; overflow: hidden;">
            ${escapeHtml(col)}
        </th>`
    ).join('');

    const bodyRows = data.map(row => {
        const cells = columns.map((col, colIndex) => {
            const value = row[colIndex];  // ✅ index-based access
            return `<td class="${getValueClass(value)}" data-column="${col}">${formatValue(value)}</td>`;
        }).join('');
        return `<tr>${cells}</tr>`;
    }).join('');

    return `
        <div class="table-container">
            <table class="data-table" id="data-table">
                <thead><tr>${headerRow}</tr></thead>
                <tbody>${bodyRows}</tbody>
            </table>
        </div>
    `;
}

        const table = document.getElementById('data-table');
        enableColumnResize(table); // this will make the columns resizable

        let draggedColumn = null;
        let columnOrder = [];
        
        function getValueClass(value) {
            if (value === null || value === undefined) return 'null-value';
            if (typeof value === 'number') return 'number-value';
            if (typeof value === 'string') return 'string-value';
            return '';
        }
        function handleDragStart(e) {
        draggedColumn = e.target.dataset.column;
        e.target.classList.add('dragging');
        e.dataTransfer.effectAllowed = 'move';
        }
        
        function formatValue(value) {
            if (value === null || value === undefined) return '<span class="null-value">NULL</span>';
            if (typeof value === 'boolean') return value ? 'true' : 'false';
            if (typeof value === 'string') return escapeHtml(value);
            return escapeHtml(String(value));
        }
        
        function handleDragOver(e) {
        e.preventDefault();
        e.dataTransfer.dropEffect = 'move';
        e.target.classList.add('drop-target');
        }
        
        function handleDrop(e) {
            e.preventDefault();
            const targetColumn = e.target.dataset.column;
            
            if (draggedColumn && targetColumn && draggedColumn !== targetColumn) {
                reorderColumns(draggedColumn, targetColumn);
            }
            
            // Remove all visual indicators
            document.querySelectorAll('.drop-target').forEach(el => el.classList.remove('drop-target'));
            }
        function handleDragEnd(e) {
            e.target.classList.remove('dragging');
            document.querySelectorAll('.drop-target').forEach(el => el.classList.remove('drop-target'));
            draggedColumn = null;
        }
        
        function reorderColumns(draggedCol, targetCol) {
        if (!currentResults || currentResults.length === 0) return;
        
        // Get current column order
        const columns = Object.keys(currentResults[0]);
        const draggedIndex = columns.indexOf(draggedCol);
        const targetIndex = columns.indexOf(targetCol);
        
        // Reorder columns array
        columns.splice(draggedIndex, 1);
        columns.splice(targetIndex, 0, draggedCol);
        
        // Reorder the data to match new column order
        const reorderedData = currentResults.map(row => {
            const newRow = {};
            columns.forEach(col => {
                newRow[col] = row[col];
            });
            return newRow;
    });
    
    // Update the display
        currentResults = reorderedData;
        const content = document.getElementById('results-content');
        const tableHtml = createDataTable(currentResults);
        const queryInfo = content.querySelector('.query-info');
        content.innerHTML = tableHtml + (queryInfo ? queryInfo.outerHTML : '');
    }
        function escapeHtml(text) {
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }
        
        async function refreshTables() {
            const tablesList = document.getElementById('tables-list');
            tablesList.innerHTML = '<div class="empty-state"><span class="loading-spinner"></span>Loading tables...</div>';
            
            try {
                const response = await fetch('/api/tables');
                const result = await response.json();
                
                if (result.success && result.tables.length > 0) {
                    const tablesHtml = result.tables.map(table => `
                        <div class="table-item" onclick="insertTableName('${table.name}')">
                            <div class="table-name">${table.name}</div>
                            <div class="table-info">${table.rows} rows • ${table.columns} columns</div>
                        </div>
                    `).join('');
                    tablesList.innerHTML = tablesHtml;
                } else {
                    tablesList.innerHTML = '<div class="empty-state"><p>No tables found</p></div>';
                }
                
            } catch (error) {
                console.error('Error loading tables:', error);
                tablesList.innerHTML = '<div class="empty-state"><p>Error loading tables</p></div>';
            }
        }
        
        function insertTableName(tableName) {
            if (editor) {
                editor.replaceSelection(tableName);
                editor.focus();
            }
        }
        
        function showMessage(message, type = 'info') {
            const content = document.getElementById('results-content');
            const messageClass = `message-${type}`;
            content.innerHTML = `<div class="message ${messageClass}">${escapeHtml(message)}</div>`;
        }
        
        function updateStatus(message) {
            document.getElementById('status-text').textContent = message;
        }
        
        function clearEditor() {
            if (editor) {
                editor.setValue('');
                editor.focus();
            }
        }
        
        function clearAll() {
            clearEditor();
            const content = document.getElementById('results-content');
            content.innerHTML = `
                <div class="empty-state">
                    <span>📊</span>
                    <p>Execute a query to see results here</p>
                </div>
            `;
            currentResults = [];
            updateStatus('Ready');
        }
    </script>
</body>
</html>
"""

# UPDATED FUNCTIONS FOR NEW DATABASE STRUCTURE
def show_tables_info(database):
    """Generate table information for new Table objects"""
    tables = []
    for name, table_obj in database.items():
        if hasattr(table_obj, 'rows'):
            # Count actual data rows
            data_rows = len(table_obj.rows)
            # Get column count from schema
            columns = len(table_obj.schema) if hasattr(table_obj, 'schema') else 0
        else:
            data_rows = 0
            columns = 0
        
        tables.append({
            "name": name, 
            "rows": data_rows,
            "columns": columns
        })
    return tables

def split_queries(query_text):
    """Split multiple queries, ignoring comments"""
    # Remove single-line comments (-- comment)
    query_text = re.sub(r'--.*', '', query_text, flags=re.MULTILINE)
    
    # Remove multi-line comments (/* comment */)
    query_text = re.sub(r'/\*.*?\*/', '', query_text, flags=re.DOTALL)
    
    # Split by semicolon and filter empty queries
    queries = [q.strip() for q in query_text.split(';') if q.strip()]
    return queries

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/execute', methods=['POST'])
def execute_query():
    try:
        data = request.get_json()
        query = data.get('query', '').strip()
        
        if not query:
            return jsonify({"success": False, "error": "Empty query"})
        
        queries = split_queries(query)
        if len(queries) > 1:
            last_result = None
            for q in queries:
                result = execute_single_query(q)
                if not result["success"]:
                    return jsonify(result)
                last_result = result
            return jsonify(last_result)
        else:
            result = execute_single_query(query)
            return jsonify(result)
            
    except Exception as e:
        print(f"Error executing query: {e}")
        print(traceback.format_exc())
        return jsonify({
            "success": False,
            "error": str(e)
        })

def execute_single_query(query):
    try:
        import time
        start_time = time.time()
        
        # Remove comments before processing
        cleaned_query = re.sub(r'--.*', '', query, flags=re.MULTILINE)
        cleaned_query = re.sub(r'/\*.*?\*/', '', cleaned_query, flags=re.DOTALL)
        cleaned_query = cleaned_query.strip()
        
        if not cleaned_query:
            return {"success": False, "error": "Empty query after removing comments"}
        
        lexer = Lexer(cleaned_query)
        parser = Parser(lexer.tokens)
        
        first_token = lexer.tokens[0][0] if lexer.tokens else ""
        
        if first_token == "SELECT":
            ast = parser.parse_select_statement()
            rows = execute(ast, db_manager.active_db)
            rows_len = len(rows)
            if rows_len >= 1:
                msg = "Query executed successfully"
            else:
                msg = "Query executed successfully, But No Rows Found"
            table_name = ast.table

            if table_name in db_manager.active_db:
                table_obj = db_manager.active_db[table_name]

                if rows and len(rows) > 0:
                    if hasattr(table_obj, 'schema') and table_obj.schema:
                        # Get original column order from table schema
                        ordered_cols = list(table_obj.schema.keys())

                        # Reorder each row to match schema order, return as lists
                        ordered_rows = []
                        for row in rows:
                            ordered_row = [row.get(col, None) for col in ordered_cols]
                            ordered_rows.append(ordered_row)
                        rows = ordered_rows
                    else:
                        # No schema → fallback to first row keys
                        ordered_cols = list(rows[0].keys())
                        rows = [[row[col] for col in ordered_cols] for row in rows]
                else:
                    ordered_cols = []
                    rows = []
            else:
                ordered_cols = []
                rows = []

            get_current_database()
            execution_time = f"{(time.time() - start_time) * 1000:.2f}ms"

            return {
                "success": True,
                "columns": ordered_cols,  # guaranteed schema order
                "data": rows,             # list of lists, aligned with columns
                "message": msg,
                "execution_time": execution_time
            }


        elif first_token == "USE":
            ast = parser.parse_use_statement()
            import io, sys
            old_stdout = sys.stdout
            sys.stdout = captured_output = io.StringIO()
            
            try:
                # FIXED: Pass current database, but the parser will update db_manager
                execute(ast, get_current_database())
                message = captured_output.getvalue().strip()
            finally:
                sys.stdout = old_stdout
            execution_time = f"{(time.time() - start_time) * 1000:.2f}ms"
            
            return {
                "success": True,
                "message": message or "Database switched successfully",
                "execution_time": execution_time
            }
        
        elif first_token == "CREATE":
            second_token = lexer.tokens[1][0]
            if second_token == "DATABASE":
                
                ast = parser.parse_create_database()
                get_tables()
                import io, sys
                old_stdout = sys.stdout
                sys.stdout = captured_output = io.StringIO()
                
                try:
                    execute(ast, get_current_database())
                    message = captured_output.getvalue().strip()
                finally:
                    sys.stdout = old_stdout
                
                execution_time = f"{(time.time() - start_time) * 1000:.2f}ms"
                get_tables()
                return {
                    "success": True,
                    "message": message or "Database Created successfully",
                    "execution_time": execution_time
                }
            elif second_token == "TABLE":
                ast = parser.parse_create_table()
                import io, sys
                old_stdout = sys.stdout
                sys.stdout = captured_output = io.StringIO()
                
                try:
                    execute(ast, get_current_database())
                    message = captured_output.getvalue().strip()
                finally:
                    sys.stdout = old_stdout
                
                execution_time = f"{(time.time() - start_time) * 1000:.2f}ms"
                
                return {
                    "success": True,
                    "message": message or "Table Created successfully",
                    "execution_time": execution_time
                }
                
        
        elif first_token == "INSERT":
            ast = parser.parse_insert_statement()
            import io, sys
            old_stdout = sys.stdout
            sys.stdout = captured_output = io.StringIO()
            
            try:
                db_manager.active_db= get_current_database()
                execute(ast, db_manager.active_db)
                # Save db_manager.active_dbafter INSERT
                if hasattr(db_manager, 'save_database_file'):
                    db_manager.save_database_file()
                message = captured_output.getvalue().strip()
            finally:
                sys.stdout = old_stdout
            
            execution_time = f"{(time.time() - start_time) * 1000:.2f}ms"
            
            return {
                "success": True,
                "message": message or "Row inserted successfully",
                "execution_time": execution_time
            }
        
        elif first_token == "UPDATE":
            ast = parser.parse_update_statement()
            import io, sys
            old_stdout = sys.stdout
            sys.stdout = captured_output = io.StringIO()
            
            try:
                execute(ast, db_manager.active_db)
                # Save db_manager.active_dbafter UPDATE
                if hasattr(db_manager, 'save_database_file'):
                    db_manager.save_database_file()
                message = captured_output.getvalue().strip()
            finally:
                sys.stdout = old_stdout
            
            execution_time = f"{(time.time() - start_time) * 1000:.2f}ms"
            
            return {
                "success": True,
                "message": message or "Rows updated successfully",
                "execution_time": execution_time
            }
        elif first_token == "DELETE":
            ast = parser.parse_delete_statement()
            import io, sys
            old_stdout = sys.stdout
            sys.stdout = captured_output = io.StringIO()
            
            try:
                execute(ast, db_manager.active_db)
                # Save db_manager.active_dbafter DELETE
                if hasattr(db_manager, 'save_database_file'):
                    db_manager.save_database_file()
                message = captured_output.getvalue().strip()
            finally:
                sys.stdout = old_stdout
            
            execution_time = f"{(time.time() - start_time) * 1000:.2f}ms"
            
            return {
                "success": True,
                "message": message or "Rows deleted successfully",
                "execution_time": execution_time
            }
        
        else:
            return {
                "success": False,
                "error": f"Unsupported SQL command: {lexer.tokens[0][1]}"
            }
    
    except ValueError as ve:
        return {"success": False, "error": str(ve)}
    except KeyError as ke:
        return {"success": False, "error": f"Table/column {ke} not found"}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.route('/api/tables', methods=['GET'])
def get_tables():
    try:
        db_manager.load_database_file()  
        print(db_manager.active_db_name)
        print(db_manager.active_db)
        
        tables = show_tables_info(db_manager.active_db)
        return jsonify({
            "success": True,
            "tables": tables
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        })

if __name__ == '__main__':
    # print("🗄️ Starting SQL IDE Pro...")
    # print("🚀 Server running at: http://localhost:1234")
    # print("💡 Features:")
    # print("   • CodeMirror editor with SQL syntax highlighting")
    # print("   • Execute single query (Ctrl+Enter)")
    # print("   • Execute multiple queries (Ctrl+Shift+Enter)")
    # print("   • Resizable panels and table browser")
    # print("   • Professional VS Code-like interface")
    # print("   • Auto-save after INSERT/UPDATE/DELETE")
    # print("🔧 Make sure your engine.py, executor.py, sql_ast.py, and database_manager.py are in the same directory")
    app.run(host="0.0.0.0", port=1234, debug=True)