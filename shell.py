#!/usr/bin/env python3
"""
SQL Shell Pro - A professional command-line interface for your SQL engine
"""

import sys
import os
import time
import json
import csv
import readline
from importlib import reload
import database_manager
import atexit
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
import argparse

# Import your existing modules
from engine import db_manager, Lexer, Parser
from executor import execute


class Colors:
    """ANSI color codes for terminal output"""
    RESET = '\033[0m'
    BOLD = '\033[1m'
    DIM = '\033[2m'
    
    # Foreground colors
    BLACK = '\033[30m'
    RED = '\033[31m'
    GREEN = '\033[32m'
    YELLOW = '\033[33m'
    BLUE = '\033[34m'
    MAGENTA = '\033[35m'
    CYAN = '\033[36m'
    WHITE = '\033[37m'
    
    # Background colors
    BG_BLACK = '\033[40m'
    BG_RED = '\033[41m'
    BG_GREEN = '\033[42m'
    BG_YELLOW = '\033[43m'
    BG_BLUE = '\033[44m'
    BG_MAGENTA = '\033[45m'
    BG_CYAN = '\033[46m'
    BG_WHITE = '\033[47m'
    
    # Bright colors
    BRIGHT_BLACK = '\033[90m'
    BRIGHT_RED = '\033[91m'
    BRIGHT_GREEN = '\033[92m'
    BRIGHT_YELLOW = '\033[93m'
    BRIGHT_BLUE = '\033[94m'
    BRIGHT_MAGENTA = '\033[95m'
    BRIGHT_CYAN = '\033[96m'
    BRIGHT_WHITE = '\033[97m'


class Config:
    """Configuration settings for the SQL shell"""
    def __init__(self):
        self.show_query_time = True
        self.show_row_count = True
        self.max_column_width = 50
        self.null_display = 'NULL'
        self.prompt_format = '{db}> '
        self.pager = 'less'
        self.history_size = 1000
        self.auto_commit = True
        self.echo_queries = False
        self.colorize_output = True
        self.table_format = 'ascii'  # ascii, markdown, csv
        self.timing = True
        self.last_result = None  # Store last SELECT result
        self.last_columns = None
        
        # Load config from file if exists
        self.config_file = Path.home() / '.sqlshell_config.json'
        self.load_config()
    
    def load_config(self):
        """Load configuration from file"""
        try:
            if self.config_file.exists():
                with open(self.config_file, 'r') as f:
                    config_data = json.load(f)
                    for key, value in config_data.items():
                        if hasattr(self, key):
                            setattr(self, key, value)
        except Exception as e:
            print(f"Warning: Could not load config file: {e}")
    
    def save_config(self):
        """Save configuration to file"""
        try:
            config_data = {
                'show_query_time': self.show_query_time,
                'show_row_count': self.show_row_count,
                'max_column_width': self.max_column_width,
                'null_display': self.null_display,
                'prompt_format': self.prompt_format,
                'pager': self.pager,
                'history_size': self.history_size,
                'auto_commit': self.auto_commit,
                'echo_queries': self.echo_queries,
                'colorize_output': self.colorize_output,
                'table_format': self.table_format,
                'timing': self.timing
            }
            with open(self.config_file, 'w') as f:
                json.dump(config_data, f, indent=2)
        except Exception as e:
            print(f"Warning: Could not save config file: {e}")


class HistoryManager:
    """Manages command history"""
    def __init__(self, config: Config):
        self.config = config
        self.history_file = Path.home() / '.sqlshell_history'
        self.setup_history()
    
    def setup_history(self):
        """Setup readline history"""
        try:
            # Set history length
            readline.set_history_length(self.config.history_size)
            
            # Load existing history
            if self.history_file.exists():
                readline.read_history_file(str(self.history_file))
            
            # Register save function to run on exit
            atexit.register(self.save_history)
            
        except ImportError:
            print("Warning: readline not available, history disabled")
    
    def save_history(self):
        """Save command history"""
        try:
            readline.write_history_file(str(self.history_file))
        except Exception as e:
            print(f"Warning: Could not save history: {e}")


class TableFormatter:
    """Formats query results for display"""
    
    def __init__(self, config: Config):
        self.config = config
    
    def format_table(self, rows: List[Dict], columns: List[str] = None) -> str:
        """Format table data for display"""
        if not rows:
            return self._colorize("Empty result set", Colors.BRIGHT_BLACK)
        
        # Get columns from first row if not provided
        if columns is None:
            columns = list(rows[0].keys()) if rows else []
        
        if self.config.table_format == 'csv':
            return self._format_csv(rows, columns)
        elif self.config.table_format == 'markdown':
            return self._format_markdown(rows, columns)
        else:
            return self._format_ascii(rows, columns)
    
    def _format_ascii(self, rows: List[Dict], columns: List[str]) -> str:
        """Format as ASCII table"""
        if not rows:
            return self._colorize("Empty result set", Colors.BRIGHT_BLACK)
        
        # Calculate column widths
        col_widths = {}
        for col in columns:
            col_widths[col] = min(len(col), self.config.max_column_width)
            
        for row in rows:
            for col in columns:
                value_str = self._format_value(row.get(col))
                col_widths[col] = max(col_widths[col], min(len(value_str), self.config.max_column_width))
        
        lines = []
        
        # Top border
        border_parts = []
        for col in columns:
            border_parts.append('-' * (col_widths[col] + 2))
        lines.append('+' + '+'.join(border_parts) + '+')
        
        # Header
        header_parts = []
        for col in columns:
            header_text = col[:col_widths[col]].ljust(col_widths[col])
            if self.config.colorize_output:
                header_text = self._colorize(header_text, Colors.BOLD + Colors.CYAN)
            header_parts.append(f' {header_text} ')
        lines.append('|' + '|'.join(header_parts) + '|')
        
        # Header separator
        lines.append('+' + '+'.join(border_parts) + '+')
        
        # Data rows
        for row in rows:
            row_parts = []
            for col in columns:
                value = row.get(col)
                value_str = self._format_value(value)
                
                # Truncate if too long
                if len(value_str) > col_widths[col]:
                    value_str = value_str[:col_widths[col]-3] + '...'
                
                value_str = value_str.ljust(col_widths[col])
                
                # Colorize based on data type
                if self.config.colorize_output:
                    value_str = self._colorize_value(value_str, value)
                
                row_parts.append(f' {value_str} ')
            lines.append('|' + '|'.join(row_parts) + '|')
        
        # Bottom border
        lines.append('+' + '+'.join(border_parts) + '+')
        
        return '\n'.join(lines)
    
    def _format_markdown(self, rows: List[Dict], columns: List[str]) -> str:
        """Format as Markdown table"""
        if not rows:
            return "| Empty result set |"
        
        lines = []
        
        # Header
        header = '| ' + ' | '.join(columns) + ' |'
        lines.append(header)
        
        # Separator
        separator = '| ' + ' | '.join(['---'] * len(columns)) + ' |'
        lines.append(separator)
        
        # Data rows
        for row in rows:
            row_data = []
            for col in columns:
                value = self._format_value(row.get(col))
                row_data.append(value)
            lines.append('| ' + ' | '.join(row_data) + ' |')
        
        return '\n'.join(lines)
    
    def _format_csv(self, rows: List[Dict], columns: List[str]) -> str:
        """Format as CSV"""
        lines = []
        
        # Header
        lines.append(','.join(columns))
        
        # Data rows
        for row in rows:
            row_data = []
            for col in columns:
                value = self._format_value(row.get(col))
                if ',' in value or '"' in value or '\n' in value:
                    value = '"' + value.replace('"', '""') + '"'
                row_data.append(value)
            lines.append(','.join(row_data))
        
        return '\n'.join(lines)
    
    def _format_value(self, value) -> str:
        """Format a single value for display"""
        if value is None:
            return self.config.null_display
        elif isinstance(value, bool):
            return 'TRUE' if value else 'FALSE'
        elif isinstance(value, (int, float)):
            return str(value)
        else:
            return str(value)
    
    def _colorize_value(self, text: str, value) -> str:
        """Apply color based on value type"""
        if value is None:
            return self._colorize(text, Colors.BRIGHT_BLACK)
        elif isinstance(value, bool):
            return self._colorize(text, Colors.BRIGHT_MAGENTA)
        elif isinstance(value, (int, float)):
            return self._colorize(text, Colors.BRIGHT_GREEN)
        elif isinstance(value, str):
            return self._colorize(text, Colors.WHITE)
        else:
            return text
    
    def _colorize(self, text: str, color: str) -> str:
        """Apply color to text"""
        if self.config.colorize_output:
            return f"{color}{text}{Colors.RESET}"
        return text


class SQLShell:
    """Main SQL shell class"""
    
    def __init__(self):
        self.config = Config()
        self.history = HistoryManager(self.config)
        self.formatter = TableFormatter(self.config)
        self.query_count = 0
        self.start_time = datetime.now()
        
        # Command mappings
        self.meta_commands = {
            '\\q': self._cmd_quit,
            '\\quit': self._cmd_quit,
            '\\h': self._cmd_help,
            '\\help': self._cmd_help,
            '\\l': self._cmd_list_databases,
            '\\list': self._cmd_list_databases,
            '\\d': self._cmd_describe_tables,
            '\\dt': self._cmd_describe_tables,
            '\\c': self._cmd_connect,
            '\\connect': self._cmd_connect,
            '\\timing': self._cmd_toggle_timing,
            '\\echo': self._cmd_toggle_echo,
            '\\set': self._cmd_set_config,
            '\\show': self._cmd_show_config,
            '\\history': self._cmd_show_history,
            '\\clear': self._cmd_clear,
            '\\export': self._cmd_export,
            '\\import': self._cmd_import,
            '\\version': self._cmd_version,
            '\\status': self._cmd_status,
        }
    
    def print_banner(self):
        """Print welcome banner"""
        banner = f"""

{Colors.BRIGHT_GREEN}Version 2.0{Colors.RESET} - Type {Colors.BOLD}\\h{Colors.RESET} for help, {Colors.BOLD}\\q{Colors.RESET} to quit

{self._get_connection_info()}
"""
        print(banner)
    
    def _get_connection_info(self) -> str:
        """Get current connection information"""
        current_db = getattr(db_manager, 'active_db_name', None)
        if current_db:
            import os
            filename = os.path.basename(current_db)
            if filename.endswith('.su'):
                filename = filename[:-3]
            return f"{Colors.GREEN}Connected to database: {Colors.BOLD}{filename}{Colors.RESET}"
        else:
            return f"{Colors.YELLOW}No database selected. Use {Colors.BOLD}USE database_name{Colors.RESET}{Colors.YELLOW} to connect.{Colors.RESET}"
    def get_prompt(self) -> str:
        """Generate the command prompt"""
        db_name = getattr(db_manager, 'active_db_name', None) or 'none'
        
        # Extract just the filename without path and extension
        if db_name and db_name != 'none':
            import os
            # Get just the filename from the full path
            filename = os.path.basename(db_name)
            # Remove the .su extension if present
            if filename.endswith('.su'):
                filename = filename[:-3]
            db_name = filename
        
        prompt = self.config.prompt_format.format(db=db_name)
        
        if self.config.colorize_output:
            return f"{Colors.BOLD + Colors.BLUE}{prompt}{Colors.RESET}"
        return prompt


    def run(self):
        # reload(database_manager)
        """Main shell loop"""
        self.print_banner()
        
        query_buffer = []
        
        while True:
            try:
                # Determine prompt (continuation or normal)
                if query_buffer:
                    prompt = f"{Colors.DIM}    -> {Colors.RESET}" if self.config.colorize_output else "    -> "
                else:
                    prompt = self.get_prompt()
                
                line = input(prompt).strip()
                
                # Handle empty lines
                if not line:
                    continue
                
                # Handle meta-commands
                if line.startswith('\\'):
                    if query_buffer:
                        print(f"{Colors.YELLOW}Warning: Discarding incomplete query{Colors.RESET}")
                        query_buffer = []
                    
                    self._handle_meta_command(line)
                    continue
                
                # Add line to query buffer
                query_buffer.append(line)
                
                # Check if query is complete (ends with semicolon)
                full_query = ' '.join(query_buffer)
                if full_query.rstrip().endswith(';'):
                    self._execute_query(full_query)
                    query_buffer = []
                
            except KeyboardInterrupt:
                print(f"\n{Colors.YELLOW}^C{Colors.RESET}")
                query_buffer = []
                continue
            except EOFError:
                break
            except Exception as e:
                print(f"{Colors.RED}Error: {e}{Colors.RESET}")
    
    def _handle_meta_command(self, command: str):
        """Handle meta-commands (starting with \\)"""
        parts = command.split()
        cmd = parts[0].lower()
        args = parts[1:] if len(parts) > 1 else []
        
        if cmd in self.meta_commands:
            try:
                self.meta_commands[cmd](args)
            except Exception as e:
                print(f"{Colors.RED}Error executing command: {e}{Colors.RESET}")
        else:
            print(f"{Colors.RED}Unknown command: {cmd}{Colors.RESET}")
            print(f"Type {Colors.BOLD}\\h{Colors.RESET} for help")
    
    def _execute_query(self, query: str):
        """Execute a SQL query using your existing Lexer/Parser/execute architecture"""
        query = query.strip()
        if not query:
            return
        
        
        if self.config.echo_queries:
            print(f"{Colors.DIM}{query};{Colors.RESET}")
        
        start_time = time.time()
        
        try:
            # Get current database
            database = db_manager.active_db
            
            # Use your existing Lexer and Parser
            lexer = Lexer(query)
            parser = Parser(lexer.tokens)
            
            if not lexer.tokens:
                return  # skip empty input
            
            token_type = lexer.tokens[0][0]
            next_token_type = lexer.tokens[1][0] if len(lexer.tokens) > 1 else None
            
            # Parse based on query type and execute
            if token_type == "SELECT":
                ast = parser.parse_select_statement()
                result = execute(ast, database)
                self._handle_select_result(result, start_time)
                
            elif token_type == "INSERT":
                ast = parser.parse_insert_statement()
                result = execute(ast, database)
                db_manager.save_database_file()
                self._handle_modify_result("INSERT", start_time)
                
            elif token_type == "UPDATE":
                ast = parser.parse_update_statement()
                result = execute(ast, database)
                db_manager.save_database_file()
                self._handle_modify_result("UPDATE",  start_time)
                
            elif token_type == "DELETE":
                ast = parser.parse_delete_statement()
                result = execute(ast, database)
                db_manager.save_database_file()
                self._handle_modify_result("DELETE",  start_time)
                
            elif token_type == "CREATE":
                if next_token_type == "DATABASE":
                    ast = parser.parse_create_database()
                    result = execute(ast, database)
                    self._handle_ddl_result("CREATE DATABASE",  start_time)
                elif next_token_type == "TABLE":
                    ast = parser.parse_create_table()
                    print(ast)
                    db_manager.save_database_file()
                    self._handle_ddl_result("CREATE TABLE",  start_time)
                else:
                    print(f"{Colors.RED}ERROR: Unsupported CREATE statement{Colors.RESET}")
                    
            elif token_type == "DROP":
                if next_token_type == "DATABASE":
                    ast = parser.parse_drop_database()
                    result = execute(ast, database)
                    self._handle_ddl_result("DROP DATABASE", start_time)
                elif next_token_type == "TABLE":
                    ast = parser.parse_drop_table()
                    result = execute(ast, database)
                    db_manager.save_database_file()
                    self._handle_ddl_result("DROP TABLE",  start_time)
                else:
                    print(f"{Colors.RED}ERROR: Unsupported DROP statement{Colors.RESET}")
                    
            elif token_type == "ALTER":
                ast = parser.parse_alter_statement()
                result = execute(ast, database)
                db_manager.save_database_file()
                self._handle_ddl_result("ALTER TABLE",  start_time)
                
            elif token_type == "USE":
                ast = parser.parse_use_statement()
                result = execute(ast, database)
                self._handle_use_result("USE", start_time)
                
            else:
                print(f"{Colors.RED}ERROR: Unsupported statement type '{token_type}'{Colors.RESET}")
            
            execution_time = time.time() - start_time
            self.query_count += 1
            
        except Exception as e:
            execution_time = time.time() - start_time
            print(f"{Colors.RED}ERROR: {str(e)}{Colors.RESET}")
            if self.config.timing:
                time_str = f"{execution_time:.3f}s" if execution_time < 1 else f"{execution_time:.2f}s"
                print(f"{Colors.BRIGHT_BLACK}Time: {time_str}{Colors.RESET}")
        
        print()  # Empty line for readability
    
    def _handle_select_result(self, result, start_time):
        """Handle SELECT query results"""
        execution_time = time.time() - start_time
        
        if result:  # Assuming result is a list of dictionaries for SELECT
            # Format and display table
            formatted_table = self.formatter.format_table(result)
            print(formatted_table)
            
            # Show summary
            if self.config.show_row_count:
                row_count = len(result)
                plural = 's' if row_count != 1 else ''
                print(f"\n{Colors.BRIGHT_BLACK}({row_count} row{plural}){Colors.RESET}")
        else:
            print(f"{Colors.BRIGHT_BLACK}Empty result set{Colors.RESET}")
        
        if self.config.timing:
            time_str = f"{execution_time:.3f}s" if execution_time < 1 else f"{execution_time:.2f}s"
            print(f"{Colors.BRIGHT_BLACK}Time: {time_str}{Colors.RESET}")
    
    def _handle_modify_result(self, operation,  start_time):
        """Handle INSERT/UPDATE/DELETE results"""
        execution_time = time.time() - start_time
        
        # Assuming your execute function returns some indication of success
  
        print(f"{Colors.GREEN}{operation} completed successfully{Colors.RESET}")

        if self.config.timing:
            time_str = f"{execution_time:.3f}s" if execution_time < 1 else f"{execution_time:.2f}s"
            print(f"{Colors.BRIGHT_BLACK}Time: {time_str}{Colors.RESET}")
    
    def _handle_ddl_result(self, operation, start_time):
        """Handle CREATE/DROP/ALTER results"""
        execution_time = time.time() - start_time
        
        print(f"\n{Colors.GREEN}{operation} completed successfully{Colors.RESET}\n")
        
        if self.config.timing:
            time_str = f"{execution_time:.3f}s" if execution_time < 1 else f"{execution_time:.2f}s"
            print(f"{Colors.BRIGHT_BLACK}Time: {time_str}{Colors.RESET}")
    
    def _handle_use_result(self, results, start_time):
        execution_time = time.time() - start_time
        
        # Extract clean database name for display
        current_db_name = getattr(db_manager, 'active_db_name', 'unknown')
        if current_db_name and current_db_name != 'unknown':
            import os
            filename = os.path.basename(current_db_name)
            if filename.endswith('.su'):
                filename = filename[:-3]
            current_db_name = filename
        
        print(f"\n{Colors.GREEN}Switched to database: {Colors.BOLD}{current_db_name}{Colors.RESET}\n")
        
        if self.config.timing:
            time_str = f"{execution_time:.3f}s" if execution_time < 1 else f"{execution_time:.2f}s"
            print(f"{Colors.BRIGHT_BLACK}Time: {time_str}{Colors.RESET}")
    
    # Meta-command implementations
    def _cmd_quit(self, args):
        """Quit the shell"""
        self.config.save_config()
        sys.exit(0)
    
    def _cmd_help(self, args):
        """Show help information"""
        help_text = f"""
{Colors.BOLD + Colors.CYAN}SQL Shell Pro - Help{Colors.RESET}

{Colors.BOLD}Meta Commands:{Colors.RESET}
  \\h, \\help              Show this help
  \\q, \\quit              Quit the shell
  \\l, \\list              List all databases
  \\d, \\dt                List tables in current database
  \\c <db>, \\connect <db>  Connect to database
  \\clear                 Clear screen
  \\timing                Toggle query timing on/off
  \\echo                  Toggle query echo on/off
  \\set <option> <value>  Set configuration option
  \\show [option]         Show configuration
  \\history               Show command history
  \\export <format> <file> Export last result
  \\import <file>         Import SQL file
  \\version               Show version information
  \\status                Show connection status

{Colors.BOLD}Configuration Options:{Colors.RESET}
  table_format           ascii, markdown, csv
  max_column_width       Maximum width for table columns
  null_display           How to display NULL values
  colorize_output        Enable/disable colors (true/false)

{Colors.BOLD}Query Execution:{Colors.RESET}
  - End queries with semicolon (;)
  - Multi-line queries supported
  - Ctrl+C cancels current query
  - Ctrl+D or \\q to exit

{Colors.BOLD}Examples:{Colors.RESET}
  SELECT * FROM users;
  \\set table_format markdown
  \\export csv results.csv
"""
        print(help_text)
    
    def _cmd_list_databases(self, args):
        """List all databases"""
        print(f"{Colors.BOLD}Available Databases:{Colors.RESET}")
        
        if not db_manager.databases:
            print("  No databases available")
            return
        
        for db_file in db_manager.databases:
            status = "offline"
            
            if db_file == db_manager.active_db_name:
                status = "current"
            else:
                # Quick check: if db has 0 tables or all tables have 0 rows → offline
                try:
                    with open(db_file, "rb") as f:
                        import msgpack
                        db_data = msgpack.unpack(f)
                    has_rows = any(tbl.get("rows") for tbl in db_data.values())
                    status = "online" if has_rows else "offline"
                except Exception:
                    status = "offline"
            
            # Pretty print with bullet
            print(f"  • {db_file} ({status})")
    
    def _cmd_describe_tables(self, args):
        """List tables in current database"""
        try:
            current_db = db_manager.active_db
            if not current_db:
                print(f"{Colors.YELLOW}No database selected{Colors.RESET}")
                return
            
            rows = []
            for table_name, table_obj in current_db.items():  # active_db is dict: name → Table
                row_count = len(table_obj.rows) if hasattr(table_obj, 'rows') else 0
                col_count = len(table_obj.schema) if hasattr(table_obj, 'schema') else 0
                rows.append({
                    'Table': table_name,
                    'Rows': row_count,
                    'Columns': col_count
                })
            
            if rows:
                formatted_table = self.formatter.format_table(rows, ['Table', 'Rows', 'Columns'])
                print(formatted_table)
            else:
                print(f"{Colors.YELLOW}No tables found{Colors.RESET}")

        except Exception as e:
            print(f"{Colors.RED}Error listing tables: {e}{Colors.RESET}")
    
    def _cmd_connect(self, args):
        """Connect to a database"""
        if not args:
            print(f"{Colors.RED}Usage: \\c <database_name>{Colors.RESET}")
            return
        
        db_name = args[0]
        try:
            # Execute USE statement
            self._execute_query(f"USE {db_name}")
        except Exception as e:
            print(f"{Colors.RED}Connection failed: {e}{Colors.RESET}")
    
    def _cmd_toggle_timing(self, args):
        """Toggle query timing"""
        self.config.timing = not self.config.timing
        status = "enabled" if self.config.timing else "disabled"
        print(f"Timing is {status}")
    
    def _cmd_toggle_echo(self, args):
        """Toggle query echo"""
        self.config.echo_queries = not self.config.echo_queries
        status = "enabled" if self.config.echo_queries else "disabled"
        print(f"Query echo is {status}")
    
    def _cmd_set_config(self, args):
        """Set configuration option"""
        if len(args) < 2:
            print(f"{Colors.RED}Usage: \\set <option> <value>{Colors.RESET}")
            return
        
        option, value = args[0], args[1]
        
        try:
            if option == 'table_format' and value in ['ascii', 'markdown', 'csv']:
                self.config.table_format = value
            elif option == 'max_column_width':
                self.config.max_column_width = int(value)
            elif option == 'null_display':
                self.config.null_display = value
            elif option == 'colorize_output':
                self.config.colorize_output = value.lower() == 'true'
            else:
                print(f"{Colors.RED}Unknown option: {option}{Colors.RESET}")
                return
            
            print(f"Set {option} = {value}")
            self.config.save_config()
        
        except ValueError as e:
            print(f"{Colors.RED}Invalid value: {e}{Colors.RESET}")
    
    def _cmd_show_config(self, args):
        """Show configuration"""
        if args:
            option = args[0]
            if hasattr(self.config, option):
                value = getattr(self.config, option)
                print(f"{option} = {value}")
            else:
                print(f"{Colors.RED}Unknown option: {option}{Colors.RESET}")
        else:
            print(f"{Colors.BOLD}Current Configuration:{Colors.RESET}")
            print(f"table_format = {self.config.table_format}")
            print(f"max_column_width = {self.config.max_column_width}")
            print(f"null_display = {self.config.null_display}")
            print(f"colorize_output = {self.config.colorize_output}")
            print(f"timing = {self.config.timing}")
            print(f"echo_queries = {self.config.echo_queries}")
    
    def _cmd_show_history(self, args):
        """Show command history"""
        try:
            for i in range(readline.get_current_history_length()):
                line = readline.get_history_item(i + 1)
                if line:
                    print(f"{i+1:4}: {line}")
        except:
            print("History not available")
    
    def _cmd_clear(self, args):
        """Clear the screen"""
        os.system('cls' if os.name == 'nt' else 'clear')
    
    def _cmd_export(self, args):
        """Export last query result"""
        if len(args) < 2:
            print(f"{Colors.RED}Usage: \\export <format> <filename>{Colors.RESET}")
            print(f"{Colors.YELLOW}Supported formats: csv, json, sql{Colors.RESET}")
            return
        
        if not self.last_result:
            print(f"{Colors.RED}No query result to export. Run a SELECT query first.{Colors.RESET}")
            return
        
        format_type = args[0].lower()
        filename = args[1]
        
        try:
            if format_type == 'csv':
                self._export_csv(filename)
            elif format_type == 'json':
                self._export_json(filename)
            elif format_type == 'sql':
                self._export_sql(filename)
            else:
                print(f"{Colors.RED}Unsupported format: {format_type}{Colors.RESET}")
                print(f"{Colors.YELLOW}Supported formats: csv, json, sql{Colors.RESET}")
                return
            
            row_count = len(self.last_result)
            print(f"{Colors.GREEN}Exported {row_count} rows to {filename} ({format_type.upper()} format){Colors.RESET}")
        except Exception as e:
            print(f"{Colors.RED}Export failed: {str(e)}{Colors.RESET}")

            
    def _export_csv(self, filename):
        """Export to CSV format"""
        import csv
        
        if not filename.endswith('.csv'):
            filename += '.csv'
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            if not self.last_result:
                return
            
            # Get column names from first row
            columns = list(self.last_result[0].keys())
            writer = csv.writer(csvfile)
            
            # Write header
            writer.writerow(columns)
            
            # Write data rows
            for row in self.last_result:
                row_data = [row.get(col, '') for col in columns]
                writer.writerow(row_data)
                
    def _handle_select_result(self, result, start_time):
        """Handle SELECT query results"""
        execution_time = time.time() - start_time
        
        # Store result for export functionality
        self.last_result = result
        
        if result:  # Assuming result is a list of dictionaries for SELECT
            # Format and display table
            formatted_table = self.formatter.format_table(result)
            print(formatted_table)
            
            # Show summary
            if self.config.show_row_count:
                row_count = len(result)
                plural = 's' if row_count != 1 else ''
                print(f"\n{Colors.BRIGHT_BLACK}({row_count} row{plural}){Colors.RESET}")
        else:
            print(f"{Colors.BRIGHT_BLACK}Empty result set{Colors.RESET}")
            self.last_result = None  # No data to export
        
        if self.config.timing:
            time_str = f"{execution_time:.3f}s" if execution_time < 1 else f"{execution_time:.2f}s"
            print(f"{Colors.BRIGHT_BLACK}Time: {time_str}{Colors.RESET}")
            
    def _export_json(self, filename):
        """Export to JSON format"""
        import json
        
        if not filename.endswith('.json'):
            filename += '.json'
        
        with open(filename, 'w', encoding='utf-8') as jsonfile:
            json.dump(self.last_result, jsonfile, indent=2, default=str)
    def _export_sql(self, filename):
        """Export as SQL INSERT statements"""
        if not filename.endswith('.sql'):
            filename += '.sql'
        
        if not self.last_result:
            return
        
        # We need table name - ask user or use generic name
        table_name = input(f"Enter table name for INSERT statements (default: exported_data): ").strip()
        if not table_name:
            table_name = "exported_data"
        
        with open(filename, 'w', encoding='utf-8') as sqlfile:
            columns = list(self.last_result[0].keys())
            
            # Write header comment
            sqlfile.write(f"-- Exported data from SQL Shell Pro\n")
            sqlfile.write(f"-- Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            sqlfile.write(f"-- Rows: {len(self.last_result)}\n\n")
            
            # Write INSERT statements
            for row in self.last_result:
                values = []
                for col in columns:
                    value = row.get(col)
                    if value is None:
                        values.append('NULL')
                    elif isinstance(value, str):
                        # Escape single quotes
                        escaped_value = value.replace("'", "''")
                        values.append(f"'{escaped_value}'")
                    elif isinstance(value, bool):
                        values.append('TRUE' if value else 'FALSE')
                    else:
                        values.append(str(value))
                
                columns_str = ', '.join(columns)
                values_str = ', '.join(values)
                sqlfile.write(f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str});\n")
    def _cmd_import(self, args):
        """Import SQL file"""
        if not args:
            print(f"{Colors.RED}Usage: \\import <filename>{Colors.RESET}")
            return        
        filename = args[0]
        try:
            with open(filename, 'r') as f:
                content = f.read()
                # Execute each statement
                statements = [s.strip() for s in content.split(';') if s.strip()]
                for stmt in statements:
                    print(f"Executing: {stmt[:50]}...")
                    self._execute_query(stmt)
            print(f"Imported {len(statements)} statements from {filename}")
        except FileNotFoundError:
            print(f"{Colors.RED}File not found: {filename}{Colors.RESET}")
        except Exception as e:
            print(f"{Colors.RED}Error importing file: {e}{Colors.RESET}")
    
    def _cmd_version(self, args):
        """Show version information"""
        print(f"""
{Colors.BOLD}SQL Shell Pro{Colors.RESET}
Version: 2.0
Python: {sys.version.split()[0]}
Platform: {sys.platform}
""")
    
    def _cmd_status(self, args):
        """Show connection and session status"""
        uptime = datetime.now() - self.start_time
        current_db = getattr(db_manager, 'active_db_name', 'None')
        
        print(f"""
{Colors.BOLD}Session Status:{Colors.RESET}
Database: {current_db or 'Not connected'}
Queries executed: {self.query_count}
Session uptime: {uptime}
Timing: {'ON' if self.config.timing else 'OFF'}
Echo: {'ON' if self.config.echo_queries else 'OFF'}
Table format: {self.config.table_format}
""")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='SQL Shell Pro')
    parser.add_argument('--no-color', action='store_true', help='Disable colored output')
    parser.add_argument('--config', help='Config file path')
    parser.add_argument('--execute', '-e', help='Execute command and exit')
    
    args = parser.parse_args()
    
    shell = SQLShell()
    
    if args.no_color:
        shell.config.colorize_output = False
    
    if args.execute:
        shell._execute_query(args.execute)
        return
    
    shell.run()


if __name__ == '__main__':
    main()
