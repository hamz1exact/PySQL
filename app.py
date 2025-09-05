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
try:
    db_manager.load_database_file()  # This should load your default DB
    if not hasattr(db_manager, 'active_db_name') or not db_manager.active_db_name:
        # Set to whatever your default database name should be
        db_manager.active_db_name = "main"  # or "default" or whatever
except:
    pass
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
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.12/addon/dialog/dialog.min.css">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.12/addon/search/matchesonscrollbar.min.css">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/animate.css/4.1.1/animate.min.css">
    

    <style>
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
            background: linear-gradient(135deg, #2d2d30 0%, #3c3c3c 100%);
            border-bottom: 1px solid #007acc;
            padding: 8px 16px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            height: 40px;
            box-shadow: 0 2px 8px rgba(0, 122, 204, 0.1);
        }
        
        .title-bar h1 {
            font-size: 16px;
            font-weight: 600;
            color: #ffffff;
            display: flex;
            align-items: center;
            gap: 12px;
        }
        
        .db-info {
            background: rgba(0, 122, 204, 0.1);
            border: 1px solid #007acc;
            border-radius: 4px;
            padding: 4px 8px;
            font-size: 11px;
            color: #4fc3f7;
            margin-left: 16px;
            animation: fadeInRight 0.5s ease;
        }
        
        .title-bar-buttons {
            display: flex;
            gap: 8px;
        }
        
        .btn {
            padding: 6px 14px;
            border: 1px solid #464647;
            border-radius: 4px;
            cursor: pointer;
            font-weight: 500;
            font-size: 12px;
            transition: all 0.3s ease;
            background: #3c3c3c;
            color: #cccccc;
            font-family: inherit;
            position: relative;
            overflow: hidden;
        }
        
        .btn::before {
            content: '';
            position: absolute;
            top: 0;
            left: -100%;
            width: 100%;
            height: 100%;
            background: linear-gradient(90deg, transparent, rgba(255,255,255,0.1), transparent);
            transition: left 0.5s;
        }
        
        .btn:hover::before {
            left: 100%;
        }
        
        .btn:hover {
            background: #4a4a4a;
            border-color: #007acc;
            transform: translateY(-1px);
            box-shadow: 0 4px 12px rgba(0, 122, 204, 0.2);
        }
        
        .btn-primary {
            background: linear-gradient(135deg, #0e639c 0%, #1177bb 100%);
            border-color: #007acc;
            color: white;
        }
        
        .btn-primary:hover {
            background: linear-gradient(135deg, #1177bb 0%, #1a8bcc 100%);
            box-shadow: 0 4px 16px rgba(0, 122, 204, 0.4);
        }
        
        .btn-danger {
            background: linear-gradient(135deg, #a1260d 0%, #c5391a 100%);
            border-color: #f85149;
            color: white;
        }
        
        .btn-danger:hover {
            background: linear-gradient(135deg, #c5391a 0%, #e5452a 100%);
            box-shadow: 0 4px 16px rgba(248, 81, 73, 0.4);
        }
        
        .main-layout {
            display: flex;
            flex: 1;
            overflow: hidden;
        }
        
        .sidebar {
            width: 280px;
            background: #252526;
            border-right: 1px solid #3e3e42;
            display: flex;
            flex-direction: column;
            transition: all 0.3s ease;
            box-shadow: 2px 0 8px rgba(0, 0, 0, 0.1);
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
            background: linear-gradient(135deg, #2d2d30 0%, #3c3c3c 100%);
        }
        
        .sidebar-title {
            font-size: 14px;
            font-weight: 600;
            color: #ffffff;
        }
        
        .sidebar-content {
            flex: 1;
            overflow-y: auto;
            padding: 8px 0;
        }
        
        .table-item {
            padding: 12px 16px;
            cursor: pointer;
            border-bottom: 1px solid #2d2d30;
            transition: all 0.2s ease;
            position: relative;
        }
        
        .table-item::before {
            content: '';
            position: absolute;
            left: 0;
            top: 0;
            width: 3px;
            height: 100%;
            background: #007acc;
            transform: scaleY(0);
            transition: transform 0.2s ease;
        }
        
        .table-item:hover::before {
            transform: scaleY(1);
        }
        
        .table-item:hover {
            background: #2a2d2e;
            transform: translateX(4px);
        }
        
        .table-name {
            font-weight: 600;
            color: #4ec9b0;
            font-size: 14px;
            display: flex;
            align-items: center;
            gap: 8px;
        }
        
        .table-name::before {
            content: "🗃️";
            font-size: 12px;
        }
        
        .table-info {
            font-size: 11px;
            color: #858585;
            margin-top: 4px;
            padding-left: 20px;
        }
        
        .content-area {
            flex: 1;
            display: flex;
            flex-direction: column;
            background: #1e1e1e;
        }
        
        .toolbar {
            background: linear-gradient(135deg, #2d2d30 0%, #3c3c3c 100%);
            border-bottom: 1px solid #3e3e42;
            padding: 8px 16px;
            display: flex;
            align-items: center;
            gap: 8px;
            height: 45px;
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
        }
        
        .toolbar-group {
            display: flex;
            gap: 4px;
            padding: 0 8px;
            border-right: 1px solid #464647;
        }
        
        .toolbar-group:last-child {
            border-right: none;
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
            background: linear-gradient(135deg, #2d2d30 0%, #3c3c3c 100%);
            border-bottom: 1px solid #3e3e42;
            padding: 8px 16px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            height: 35px;
        }
        
        .editor-title {
            font-size: 13px;
            color: #ffffff;
            font-weight: 600;
        }
        
        .editor-stats {
            font-size: 11px;
            color: #858585;
            display: flex;
            gap: 12px;
        }
        
        .editor-container {
            flex: 1;
            min-height: 150px;
            position: relative;
        }
        
        .CodeMirror {
            height: 100% !important;
            font-family: 'JetBrains Mono', 'Cascadia Code', 'Fira Code', 'Monaco', 'Consolas', monospace !important;
            font-size: 14px !important;
            line-height: 1.6 !important;
        }
        
        .CodeMirror-gutters {
            background: #252526 !important;
            border-right: 1px solid #3e3e42 !important;
        }
        
        .CodeMirror-linenumber {
            color: #858585 !important;
            padding: 0 8px !important;
        }
        
        .CodeMirror-activeline-background {
            background: rgba(0, 122, 204, 0.05) !important;
        }
        
        .CodeMirror-selected {
            background: rgba(0, 122, 204, 0.2) !important;
        }
        
        .resizer {
            height: 6px;
            background: linear-gradient(135deg, #2d2d30 0%, #3c3c3c 100%);
            cursor: row-resize;
            border-top: 1px solid #3e3e42;
            border-bottom: 1px solid #3e3e42;
            position: relative;
            display: flex;
            align-items: center;
            justify-content: center;
        }
        
        .resizer::after {
            content: '⋯⋯⋯';
            color: #858585;
            font-size: 12px;
            letter-spacing: 2px;
        }
        
        .resizer:hover {
            background: linear-gradient(135deg, #007acc 0%, #1177bb 100%);
        }
        
        .resizer:hover::after {
            color: white;
        }
        
        .results-panel {
            flex: 1;
            display: flex;
            flex-direction: column;
            background: #1e1e1e;
            min-height: 200px;
        }
        
        .results-header {
            background: linear-gradient(135deg, #2d2d30 0%, #3c3c3c 100%);
            border-bottom: 1px solid #3e3e42;
            padding: 8px 16px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            height: 35px;
        }
        
        .results-title {
            font-size: 13px;
            color: #ffffff;
            font-weight: 600;
        }
        
        .results-actions {
            display: flex;
            gap: 4px;
        }
        
        .results-btn {
            padding: 4px 8px;
            border: 1px solid #464647;
            border-radius: 3px;
            background: transparent;
            color: #cccccc;
            cursor: pointer;
            font-size: 11px;
            transition: all 0.2s ease;
        }
        
        .results-btn:hover {
            background: #4a4a4a;
            border-color: #007acc;
        }
        
        .results-content {
            flex: 1;
            overflow: auto;
            background: #1e1e1e;
        }
        
        .status-bar {
            background: linear-gradient(135deg, #007acc 0%, #1177bb 100%);
            color: white;
            padding: 6px 16px;
            font-size: 12px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            height: 28px;
            box-shadow: 0 -2px 8px rgba(0, 122, 204, 0.1);
        }
        
        .status-left {
            display: flex;
            align-items: center;
            gap: 16px;
        }
        
        .status-right {
            display: flex;
            align-items: center;
            gap: 16px;
        }
        
        .table-container {
            margin: 16px;
            border: 1px solid #3e3e42;
            border-radius: 8px;
            overflow: hidden;
            background: #252526;
            box-shadow: 0 4px 16px rgba(0, 0, 0, 0.2);
            animation: fadeInUp 0.3s ease;
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
            background: linear-gradient(135deg, #2d2d30 0%, #3c3c3c 100%);
            color: #4ec9b0;
            padding: 12px;
            text-align: left;
            border-bottom: 2px solid #007acc;
            border-right: 1px solid #3e3e42;
            font-weight: 600;
            font-size: 12px;
            position: sticky;
            top: 0;
            min-width: 100px;
            cursor: pointer;
            user-select: none;
            transition: all 0.2s ease;
        }
        
        .data-table th:hover {
            background: linear-gradient(135deg, #3c3c3c 0%, #4a4a4a 100%);
            color: #ffffff;
        }
        
        .data-table th.sortable::after {
            content: ' ↕';
            opacity: 0.5;
            margin-left: 4px;
        }
        
        .data-table th.sort-asc::after {
            content: ' ↑';
            opacity: 1;
            color: #007acc;
        }
        
        .data-table th.sort-desc::after {
            content: ' ↓';
            opacity: 1;
            color: #007acc;
        }
        
        .data-table td {
            padding: 10px 12px;
            border-bottom: 1px solid #2d2d30;
            border-right: 1px solid #2d2d30;
            color: #d4d4d4;
            word-break: break-word;
            transition: background 0.2s ease;
        }
        
        .data-table tr:hover {
            background: rgba(0, 122, 204, 0.1);
        }
        
        .data-table tr:nth-child(even) {
            background: rgba(255, 255, 255, 0.02);
        }
        
        .data-table tr:nth-child(even):hover {
            background: rgba(0, 122, 204, 0.1);
        }
        
        .null-value {
            color: #608b4e;
            font-style: italic;
            background: rgba(96, 139, 78, 0.1);
            padding: 2px 4px;
            border-radius: 3px;
        }
        
        .number-value {
            color: #b5cea8;
            text-align: right;
            font-weight: 500;
        }
        
        .string-value {
            color: #ce9178;
        }
        
        .boolean-value {
            color: #569cd6;
            font-weight: bold;
        }
        
        .message {
            margin: 16px;
            padding: 16px 20px;
            border-radius: 8px;
            font-size: 13px;
            display: flex;
            align-items: center;
            gap: 12px;
            animation: slideInDown 0.3s ease;
            border-left: 4px solid;
        }
        
        .message::before {
            font-size: 18px;
        }
        
        .message-success {
            background: rgba(106, 153, 85, 0.15);
            border-color: #6a9955;
            color: #6a9955;
        }
        
        .message-success::before {
            content: '✅';
        }
        
        .message-error {
            background: rgba(244, 71, 71, 0.15);
            border-color: #f44747;
            color: #f44747;
        }
        
        .message-error::before {
            content: '❌';
        }
        
        .message-info {
            background: rgba(0, 122, 204, 0.15);
            border-color: #007acc;
            color: #007acc;
        }
        
        .message-info::before {
            content: 'ℹ️';
        }
        
        .message-warning {
            background: rgba(255, 193, 7, 0.15);
            border-color: #ffc107;
            color: #ffc107;
        }
        
        .message-warning::before {
            content: '⚠️';
        }
        
        .empty-state {
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            height: 250px;
            color: #858585;
            font-size: 14px;
            gap: 12px;
        }
        
        .empty-state-icon {
            font-size: 48px;
            opacity: 0.3;
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
            font-size: 12px;
            color: #858585;
            margin: 12px 16px;
            padding: 12px 16px;
            background: linear-gradient(135deg, #2d2d30 0%, #3c3c3c 100%);
            border-radius: 6px;
            border-left: 4px solid #007acc;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        
        .query-stats {
            display: flex;
            gap: 16px;
        }
        
        .notification {
            position: fixed;
            top: 60px;
            right: 20px;
            padding: 12px 20px;
            border-radius: 6px;
            color: white;
            font-size: 13px;
            z-index: 1000;
            min-width: 300px;
            animation: slideInRight 0.3s ease;
            box-shadow: 0 4px 16px rgba(0, 0, 0, 0.3);
        }
        
        .notification.success {
            background: linear-gradient(135deg, #6a9955 0%, #7aa968 100%);
        }
        
        .notification.error {
            background: linear-gradient(135deg, #f44747 0%, #ff5757 100%);
        }
        
        .notification.info {
            background: linear-gradient(135deg, #007acc 0%, #1177bb 100%);
        }
        
        .notification.warning {
            background: linear-gradient(135deg, #ffc107 0%, #ffcd38 100%);
            color: #000;
        }
        
        .fullscreen-overlay {
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0, 0, 0, 0.8);
            display: flex;
            align-items: center;
            justify-content: center;
            z-index: 2000;
            animation: fadeIn 0.3s ease;
        }
        
        .fullscreen-table {
            background: #1e1e1e;
            border-radius: 8px;
            max-width: 95%;
            max-height: 95%;
            overflow: auto;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.5);
        }
        
        .close-fullscreen {
            position: absolute;
            top: 20px;
            right: 20px;
            background: #f44747;
            border: none;
            color: white;
            padding: 8px 12px;
            border-radius: 4px;
            cursor: pointer;
            font-size: 14px;
        }
        
        ::-webkit-scrollbar {
            width: 12px;
            height: 12px;
        }
        
        ::-webkit-scrollbar-track {
            background: #2d2d30;
            border-radius: 6px;
        }
        
        ::-webkit-scrollbar-thumb {
            background: linear-gradient(135deg, #464647 0%, #5a5a5c 100%);
            border-radius: 6px;
        }
        
        ::-webkit-scrollbar-thumb:hover {
            background: linear-gradient(135deg, #5a5a5c 0%, #6a6a6c 100%);
        }
        
        @keyframes fadeIn {
            from { opacity: 0; }
            to { opacity: 1; }
        }
        
        @keyframes fadeInUp {
            from { opacity: 0; transform: translateY(20px); }
            to { opacity: 1; transform: translateY(0); }
        }
        
        @keyframes fadeInRight {
            from { opacity: 0; transform: translateX(20px); }
            to { opacity: 1; transform: translateX(0); }
        }
        
        @keyframes slideInDown {
            from { opacity: 0; transform: translateY(-20px); }
            to { opacity: 1; transform: translateY(0); }
        }
        
        @keyframes slideInRight {
            from { opacity: 0; transform: translateX(100%); }
            to { opacity: 1; transform: translateX(0); }
        }
        
        .tooltip {
            position: relative;
        }
        
        .tooltip:hover::after {
            content: attr(data-tooltip);
            position: absolute;
            bottom: 100%;
            left: 50%;
            transform: translateX(-50%);
            background: #2d2d30;
            color: #ffffff;
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 11px;
            white-space: nowrap;
            z-index: 1000;
            border: 1px solid #007acc;
        }
    </style>
</head>
<body>
    <div class="ide-container">
        <div class="title-bar">
            <h1>
                <span>🗄️</span>
                SQL IDE Pro
                <div class="db-info" id="db-info">
                    Connected: <span id="current-database">Loading...</span>
                </div>
            </h1>
            <div class="title-bar-buttons">
                <button class="btn tooltip" onclick="toggleSidebar()" data-tooltip="Toggle database explorer">
                    📊 Toggle Tables
                </button>
                <button class="btn btn-danger tooltip" onclick="clearAll()" data-tooltip="Clear editor and results">
                    🗑️ Clear All
                </button>
            </div>
        </div>
        
        <div class="main-layout">
            <div class="sidebar" id="sidebar">
                <div class="sidebar-header">
                    <span class="sidebar-title">Database Explorer</span>
                    <button class="btn tooltip" onclick="refreshTables()" data-tooltip="Refresh tables">
                        🔄
                    </button>
                </div>
                <div class="sidebar-content" id="tables-list">
                    <div class="empty-state">
                        <span class="loading-spinner"></span>
                        <span>Loading tables...</span>
                    </div>
                </div>
            </div>
            
            <div class="content-area">
                <div class="toolbar">
                    <div class="toolbar-group">
                        <button class="btn btn-primary tooltip" onclick="executeQuery()" data-tooltip="Execute current query (Ctrl+Enter)">
                            ▶ Execute
                        </button>
                        <button class="btn tooltip" onclick="executeAllQueries()" data-tooltip="Execute all queries (Ctrl+Shift+Enter)">
                            ▶▶ Execute All
                        </button>
                    </div>
                    <div class="toolbar-group">
                        <button class="btn tooltip" onclick="clearEditor()" data-tooltip="Clear editor">
                            📝 Clear Editor
                        </button>
                        <button class="btn tooltip" onclick="showFindDialog()" data-tooltip="Find & Replace (Ctrl+F)">
                            🔍 Find
                        </button>
                    </div>
                </div>
                
                <div class="editor-panel" id="editor-panel">
                    <div class="editor-header">
                        <span class="editor-title">SQL Editor</span>
                        <div class="editor-stats">
                            <span id="cursor-pos">Ln 1, Col 1</span>
                            <span id="selection-info"></span>
                            <span id="char-count">0 chars</span>
                        </div>
                    </div>
                    <div class="editor-container">
                        <textarea id="editor"></textarea>
                    </div>
                </div>
                
                <div class="resizer" id="resizer"></div>
                
                <div class="results-panel" id="results-panel">
                    <div class="results-header">
                        <span class="results-title">Query Results</span>
                        <div class="results-actions">
                            <button class="results-btn tooltip" onclick="exportResults('csv')" data-tooltip="Export as CSV">
                                📄 CSV
                            </button>
                            <button class="results-btn tooltip" onclick="exportResults('json')" data-tooltip="Export as JSON">
                                📋 JSON
                            </button>
                        </div>
                    </div>
                    <div class="results-content" id="results-content">
                        <div class="empty-state">
                            <div class="empty-state-icon">📊</div>
                            <p>Execute a query to see results here</p>
                            <small>Use Ctrl+Enter to execute the current query</small>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        
        <div class="status-bar">
            <div class="status-left">
                <span id="status-text">Ready</span>
            </div>
            <div class="status-right">
                <span id="query-count">0 queries executed</span>
                <span>SQL IDE Pro v1.0</span>
            </div>
        </div>
    </div>

    <script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.12/codemirror.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.12/mode/sql/sql.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.12/addon/search/searchcursor.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.12/addon/search/search.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.12/addon/dialog/dialog.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.12/addon/search/matchesonscrollbar.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.12/addon/search/jump-to-line.min.js"></script>
    

    <script>
    let editor;
    let currentResults = [];
    let queryCount = 0;
    let currentSort = { column: null, direction: null };

    document.addEventListener("DOMContentLoaded", () => {
        initializeEditor();
        refreshTables();
        loadCurrentDatabase();
        setupResizer();
        setupNotifications();
    });

    function initializeEditor() {
        // Define custom SQL keywords for your engine
        const customKeywords = {
            'USE': true, 'SET': true, 'SWITCH': true,
            'SELECT': true, 'FROM': true, 'WHERE': true, 'INSERT': true, 'UPDATE': true, 'DELETE': true,
            'CREATE': true, 'DROP': true, 'ALTER': true, 'TABLE': true, 'DATABASE': true,
            'AND': true, 'OR': true, 'NOT': true, 'IN': true, 'BETWEEN': true, 'LIKE': true,
            'ORDER': true, 'BY': true, 'GROUP': true, 'HAVING': true, 'LIMIT': true, 'USE': true, "DEFAULT": true,
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
                            return "keyword";
                        }
                        if (customTypes[upperWord]) {
                            return "type";
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
    }


function exportResults(format) {
    if (!currentResults || currentResults.length === 0) {
        alert('No data to export');
        return;
    }
    
    let content, filename;
    
    if (format === 'csv') {
        // Get actual column names from table headers
        const tableHeaders = document.querySelectorAll('#data-table th');
        let columnNames = [];
        
        if (tableHeaders.length > 0) {
            columnNames = Array.from(tableHeaders).map(th => th.textContent.trim());
        } else {
            // Fallback column names if no table headers found
            const colCount = currentResults[0] ? currentResults[0].length : 0;
            for (let i = 0; i < colCount; i++) {
                columnNames.push(`column_${i + 1}`);
            }
        }
        
        // Build CSV using array approach (avoid string concatenation)
        const csvLines = [];
        
        // Add header row
        csvLines.push(columnNames.join(','));
        
        // Add all data rows
        currentResults.forEach(row => {
            // Clean each cell value and handle commas/quotes
            const cleanedRow = row.map(cell => {
                if (cell === null || cell === undefined) return '';
                let value = String(cell);
                // If value contains comma or quotes, wrap in quotes and escape quotes
                if (value.includes(',') || value.includes('"')) {
                    value = '"' + value.replace(/"/g, '""') + '"';
                }
                return value;
            });
            csvLines.push(cleanedRow.join(','));
        });
        
        content = csvLines.join('%0A');
        filename = 'query_results.csv';
        
    } else if (format === 'json') {
        // Get column names for JSON
        const tableHeaders = document.querySelectorAll('#data-table th');
        let columnNames = [];
        
        if (tableHeaders.length > 0) {
            columnNames = Array.from(tableHeaders).map(th => th.textContent.trim());
        } else {
            const colCount = currentResults[0] ? currentResults[0].length : 0;
            for (let i = 0; i < colCount; i++) {
                columnNames.push(`column_${i + 1}`);
            }
        }
        
        // Convert to JSON format
        const jsonData = currentResults.map(row => {
            const rowObject = {};
            columnNames.forEach((colName, index) => {
                rowObject[colName] = row[index];
            });
            return rowObject;
        });
        
        content = encodeURIComponent(JSON.stringify(jsonData, null, 2));
        filename = 'query_results.json';
        
    } else {
        alert('Format not supported. Use csv or json');
        return;
    }
    
    // Create and trigger download
    const element = document.createElement('a');
    element.setAttribute('href', 'data:text/plain;charset=utf-8,' + content);
    element.setAttribute('download', filename);
    element.style.display = 'none';
    document.body.appendChild(element);
    element.click();
    document.body.removeChild(element);
    
    showMessage(`Export completed: ${filename} with ${currentResults.length} rows`, 'success');
}

function showNotification(message, type = 'info', duration = 4000) {
    const container = document.getElementById('notification-container');
    if (!container) return;
    
    const notification = document.createElement('div');
    notification.className = `notification ${type}`;
    notification.textContent = message;
    
    container.appendChild(notification);
    
    // Auto remove after duration
    setTimeout(() => {
        notification.style.animation = 'slideInRight 0.3s ease reverse';
        setTimeout(() => {
            if (container.contains(notification)) {
                container.removeChild(notification);
            }
        }, 300);
    }, duration);
}
        
        function setupResizer() {
            const resizer = document.getElementById('resizer');
            const editorPanel = document.getElementById('editor-panel');
            const resultsPanel = document.getElementById('results-panel');
            let isResizing = false;
            
            resizer.addEventListener('mousedown', (e) => {
                isResizing = true;
                document.body.style.cursor = 'row-resize';
                document.addEventListener('mousemove', handleResize);
                document.addEventListener('mouseup', () => {
                    isResizing = false;
                    document.body.style.cursor = 'default';
                    document.removeEventListener('mousemove', handleResize);
                });
            });
            
        function handleResize(e) {
                if (!isResizing) return;
                
                const container = document.querySelector('.content-area');
                const containerRect = container.getBoundingClientRect();
                const mouseY = e.clientY - containerRect.top;
                const toolbarHeight = 45;
                const minHeight = 150;
                
                const editorHeight = Math.max(minHeight, mouseY - toolbarHeight);
                const totalHeight = containerRect.height - toolbarHeight - 6;
                const resultsHeight = Math.max(minHeight, totalHeight - editorHeight);
                
                editorPanel.style.height = `${editorHeight}px`;
                resultsPanel.style.height = `${resultsHeight}px`;
                
                if (editor) {
                    setTimeout(() => editor.refresh(), 0);
                }
            }
        }
    async function loadCurrentDatabase() {
        try {
            const response = await fetch('/api/current-database');
            if (response.ok) {
                const result = await response.json();
                if (result.success && result.database) {
                    document.getElementById('current-database').textContent = result.database;
                }
            }
        } catch (error) {
            console.error('Error loading current database:', error);
            document.getElementById('current-database').textContent = 'Unknown';
        }
    }
        
        function setupNotifications() {
            // Create notification container if it doesn't exist
            if (!document.getElementById('notification-container')) {
                const container = document.createElement('div');
                container.id = 'notification-container';
                container.style.cssText = 'position: fixed; top: 60px; right: 20px; z-index: 1000;';
                document.body.appendChild(container);
            }
        }
        
        function showNotification(message, type = 'info', duration = 4000) {
            const container = document.getElementById('notification-container');
            const notification = document.createElement('div');
            notification.className = `notification ${type}`;
            notification.textContent = message;
            
            container.appendChild(notification);
            
            // Auto remove after duration
            setTimeout(() => {
                notification.style.animation = 'slideInRight 0.3s ease reverse';
                setTimeout(() => {
                    if (container.contains(notification)) {
                        container.removeChild(notification);
                    }
                }, 300);
            }, duration);
        }
        
        
        
        function showFindDialog() {
            CodeMirror.commands.find(editor);
        }
        function toggleSidebar() {
            const sidebar = document.getElementById('sidebar');
            sidebar.classList.toggle('collapsed');
            
            // Refresh editor after sidebar toggle
            setTimeout(() => {
                if (editor) editor.refresh();
            }, 300);
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
                showMessage('No query to execute', 'warning');
                return;
            }
            
            await runQuery(query.trim());
        }
        
        
        async function executeAllQueries() {
            const allText = editor.getValue();
            const queries = splitQueries(allText);
            
            if (queries.length === 0) {
                showMessage('No queries found', 'warning');
                return;
            }
            
            updateStatus(`Executing ${queries.length} queries...`);
            showMessage(`Executing ${queries.length} queries`, 'info');
            
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
                    queryCount++;
                    updateQueryCount();
                    updateStatus(result.message || 'Query executed successfully');

                    // ✅ Automatically refresh tables if the query affects DB structure
                    const queryType = query.trim().split(' ')[0].toUpperCase();
                    if (['USE', 'CREATE', 'DROP', 'ALTER'].includes(queryType)) {
                        refreshTables();
                    }
                     if (queryType === 'USE') {
                        loadCurrentDatabase(); // Refresh the database name
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
                    <div class="query-stats">
                        <span><strong>${result.data.length}</strong> rows returned</span>
                        <span>Query executed in <strong>${result.execution_time || 'N/A'}</strong></span>
                        <span><strong>${result.columns ? result.columns.length : 0}</strong> columns</span>
                    </div>
                    <div>
                        Query #${queryCount + 1}
                    </div>
                </div>`;
                
                content.innerHTML = tableHtml + queryInfo;
                setupTableSorting();
            } else if (result.message) {
                content.innerHTML = `<div class="message message-success">${escapeHtml(result.message)}</div>`;
            } else {
                content.innerHTML = `<div class="message message-info">Query executed successfully</div>`;
            }
        }
        
                function setupTableSorting() {
            // Table sorting is handled by the sortTable function
            currentSort = { column: null, direction: null };
        }
        
        function sortTable(columnName) {
            if (!currentResults || currentResults.length === 0) return;
            
            const columns = Array.from(document.querySelectorAll('#data-table th')).map(th => th.dataset.column);
            const columnIndex = columns.indexOf(columnName);
            
            if (columnIndex === -1) return;
            
            // Determine sort direction
            let direction = 'asc';
            if (currentSort.column === columnName && currentSort.direction === 'asc') {
                direction = 'desc';
            }
            
            // Sort the data
            const sortedData = [...currentResults].sort((a, b) => {
                const aVal = a[columnIndex];
                const bVal = b[columnIndex];
                
                // Handle null values
                if (aVal === null || aVal === undefined) return direction === 'asc' ? 1 : -1;
                if (bVal === null || bVal === undefined) return direction === 'asc' ? -1 : 1;
                
                // Compare values
                if (typeof aVal === 'number' && typeof bVal === 'number') {
                    return direction === 'asc' ? aVal - bVal : bVal - aVal;
                }
                
                const aStr = String(aVal).toLowerCase();
                const bStr = String(bVal).toLowerCase();
                
                if (direction === 'asc') {
                    return aStr < bStr ? -1 : aStr > bStr ? 1 : 0;
                } else {
                    return aStr > bStr ? -1 : aStr < bStr ? 1 : 0;
                }
            });
            
            // Update current sort state
            currentSort = { column: columnName, direction };
            
            // Update UI
            document.querySelectorAll('#data-table th').forEach(th => {
                th.className = 'sortable';
            });
            
            const sortedHeader = document.querySelector(`#data-table th[data-column="${columnName}"]`);
            if (sortedHeader) {
                sortedHeader.className = `sortable sort-${direction}`;
            }
            
            // Rebuild table body
            const tbody = document.querySelector('#data-table tbody');
            const columns_list = columns;
            
            tbody.innerHTML = sortedData.map(row => {
                const cells = columns_list.map((col, colIndex) => {
                    const value = row[colIndex];
                    return `<td class="${getValueClass(value)}" data-column="${col}">${formatValue(value)}</td>`;
                }).join('');
                return `<tr>${cells}</tr>`;
            }).join('');
            
            currentResults = sortedData;
            
            showNotification(`Sorted by ${columnName} (${direction})`, 'info', 2000);
        }


        function createDataTable(data, columns) {
            if (!data || data.length === 0) {
                return '<div class="empty-state"><div class="empty-state-icon">📊</div><p>No data to display</p></div>';
            }

            const headerRow = columns.map((col, index) =>
                `<th class="sortable" data-column="${col}" data-index="${index}" onclick="sortTable('${col}')">
                    ${escapeHtml(col)}
                </th>`
            ).join('');

            const bodyRows = data.map(row => {
                const cells = columns.map((col, colIndex) => {
                    const value = row[colIndex];
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
        
        function getValueClass(value) {
            if (value === null || value === undefined) return 'null-value';
            if (typeof value === 'number') return 'number-value';
            if (typeof value === 'boolean') return 'boolean-value';
            if (typeof value === 'string') return 'string-value';
            return '';
        }
        
        
    
        
        function formatValue(value) {
            if (value === null || value === undefined) return '<span class="null-value">NULL</span>';
            if (typeof value === 'boolean') return `<span class="boolean-value">${value ? 'TRUE' : 'FALSE'}</span>`;
            if (typeof value === 'string') return escapeHtml(value);
            return escapeHtml(String(value));
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
            tablesList.innerHTML = '<div class="empty-state"><span class="loading-spinner"></span><span>Loading tables...</span></div>';
            
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
                    tablesList.innerHTML = '<div class="empty-state"><div class="empty-state-icon">🗂️</div><p>No tables found</p><small>Create a table to get started</small></div>';
                }
                
            } catch (error) {
                console.error('Error loading tables:', error);
                tablesList.innerHTML = '<div class="empty-state"><div class="empty-state-icon">❌</div><p>Error loading tables</p></div>';
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
        
        function updateQueryCount() {
            document.getElementById('query-count').textContent = `${queryCount} queries executed`;
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
                    <div class="empty-state-icon">📊</div>
                    <p>Execute a query to see results here</p>
                    <small>Use Ctrl+Enter to execute the current query</small>
                </div>
            `;
            currentResults = [];
            queryCount = 0;
            updateQueryCount();
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

@app.route('/api/current-database', methods=['GET'])
def get_current_database_name():
    try:
        return jsonify({
            "success": True,
            "database": db_manager.active_db_name or "default"
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        })

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
                # execute(ast, get_current_database())
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
    app.run(host="0.0.0.0", port=8000, debug=False)