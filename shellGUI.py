import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import sys
import os
from database import database
from engine import Lexer, Parser
from executor import execute

class DatabaseGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("SU-SQL - Database Engine Interface")
        self.root.geometry("1400x900")  # Larger window for modern feel
        self.root.configure(bg='#0d1117')  # GitHub dark theme inspired
        
        # Set application icon
        self.set_app_icon()
        
        # Font scaling for zoom functionality
        self.font_scale = 1.0
        self.base_font_sizes = {
            'title': 20,
            'subtitle': 12,
            'query': 12,
            'message': 11,
            'button': 10,
            'label': 10,
            'table': 10
        }
        from tkinter import ttk
        
        # Modern color scheme
        self.colors = {
            'bg_primary': '#0d1117',      # Main background
            'bg_secondary': '#161b22',     # Secondary background
            'bg_tertiary': '#21262d',      # Cards/panels
            'accent_primary': '#238636',   # Green accent
            'accent_secondary': '#1f6feb', # Blue accent
            'accent_danger': '#f85149',    # Red accent
            'accent_warning': '#d29922',   # Yellow accent
            'text_primary': '#f0f6fc',     # Primary text
            'text_secondary': '#7d8590',   # Secondary text
            'border': '#30363d',           # Borders
            'hover': '#30363d',            # Hover states
            'gradient_start': '#238636',   # Gradient start
            'gradient_end': '#2ea043'      # Gradient end
        }
        
        # Configure modern theme
        self.setup_modern_theme()
        
        self.create_widgets()
        self.setup_zoom_bindings()
        
    def set_app_icon(self):
        """Set application icon - looks for icon.ico, icon.png, or logo.png in the same directory"""
        icon_files = ['icon.ico', 'icon.png', 'logo.png', 'app_icon.ico', 'app_icon.png']
        
        # Always check relative to this script’s directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        
        for icon_file in icon_files:
            icon_path = os.path.join(script_dir, icon_file)
            if os.path.exists(icon_path):
                try:
                    if icon_path.endswith('.ico'):
                        # Windows only (macOS often ignores .ico)
                        self.root.iconbitmap(icon_path)
                    else:
                        # PNG works cross-platform (especially macOS)
                        icon_image = tk.PhotoImage(file=icon_path)
                        self.root.iconphoto(True, icon_image)
                    print(f"Icon loaded: {icon_path}")
                    return
                except Exception as e:
                    print(f"Failed to load icon {icon_path}: {e}")
                    continue
        
        print("No icon file found. Looking for: " + ", ".join(icon_files))
        print("Place your icon file in the same directory as this script.")
        
    def setup_modern_theme(self):
        """Configure modern dark theme with gradients and shadows"""
        style = ttk.Style()
        style.theme_use('clam')
        # Define custom layout for Modern.TLabelframe
        

        # Configure modern colors
        style.configure('TLabel', 
                       background=self.colors['bg_primary'], 
                       foreground=self.colors['text_primary'],
                       font=('Segoe UI', 10))
        
        style.configure('TFrame', background=self.colors['bg_primary'])
        style.layout("Modern.TLabelFrame",
                 [('Labelframe.border', {'sticky': 'nswe', 'children':
                     [('Labelframe.padding', {'sticky': 'nswe', 'children':
                         [('Labelframe.label', {'sticky': 'nw'}),
                          ('Labelframe.frame', {'sticky': 'nswe'})]
                     })]
                 })])

        style.layout("Modern.TLabelFrame.Label", style.layout("TLabel"))
        
        # Modern LabelFrame with rounded appearance
        style.configure('Modern.TLabelFrame', 
                       background=self.colors['bg_tertiary'], 
                       foreground=self.colors['text_primary'],
                       borderwidth=2,
                       relief='flat')
        style.configure('Modern.TLabelFrame.Label', 
                       background=self.colors['bg_tertiary'], 
                       foreground=self.colors['text_primary'], 
                       font=('Segoe UI', 11, 'bold'))
        
        # Modern primary button (Execute)
        style.configure('Modern.Execute.TButton',
                       background=self.colors['accent_primary'],
                       foreground='white',
                       borderwidth=0,
                       focuscolor='none',
                       padding=(25, 15),
                       font=('Segoe UI', 11, 'bold'))
        
        # Modern secondary buttons
        style.configure('Modern.Secondary.TButton',
                       background=self.colors['bg_tertiary'],
                       foreground=self.colors['text_primary'],
                       borderwidth=1,
                       bordercolor=self.colors['border'],
                       focuscolor='none',
                       padding=(20, 12),
                       font=('Segoe UI', 10))
        
        # Modern utility buttons (zoom, clear)
        style.configure('Modern.Utility.TButton',
                       background=self.colors['bg_secondary'],
                       foreground=self.colors['text_secondary'],
                       borderwidth=1,
                       bordercolor=self.colors['border'],
                       focuscolor='none',
                       padding=(12, 8),
                       font=('Segoe UI', 9))
        
        # Modern notebook styling
        style.configure('Modern.TNotebook', 
                       background=self.colors['bg_tertiary'], 
                       borderwidth=0,
                       tabmargins=[0, 0, 0, 0])
        
        style.configure('Modern.TNotebook.Tab', 
                       background=self.colors['bg_secondary'], 
                       foreground=self.colors['text_secondary'], 
                       padding=[20, 12],
                       font=('Segoe UI', 10),
                       borderwidth=0)
        
        # Modern treeview with better spacing
        style.configure('Modern.Treeview', 
                       background=self.colors['bg_tertiary'], 
                       foreground=self.colors['text_primary'], 
                       fieldbackground=self.colors['bg_tertiary'], 
                       borderwidth=0,
                       rowheight=30)
        
        style.configure('Modern.Treeview.Heading', 
                       background=self.colors['bg_secondary'], 
                       foreground=self.colors['text_primary'],
                       borderwidth=0,
                       font=('Segoe UI', 10, 'bold'))
        
        # Modern hover effects
        style.map('Modern.Execute.TButton', 
                 background=[('active', self.colors['gradient_end']), 
                           ('pressed', self.colors['gradient_start'])])
        
        style.map('Modern.Secondary.TButton', 
                 background=[('active', self.colors['hover']), 
                           ('pressed', self.colors['bg_secondary'])])
        
        style.map('Modern.Utility.TButton',
                 background=[('active', self.colors['hover']),
                           ('pressed', self.colors['bg_primary'])])
        
        style.map('Modern.TNotebook.Tab', 
                 background=[('selected', self.colors['bg_tertiary']), 
                           ('active', self.colors['hover'])],
                 foreground=[('selected', self.colors['text_primary'])])
        
        style.map('Modern.Treeview', 
                 background=[('selected', self.colors['accent_secondary'])],
                 foreground=[('selected', 'white')])
        
    def setup_zoom_bindings(self):
        """Setup keyboard shortcuts for zoom functionality"""
        self.root.bind('<Control-plus>', self.zoom_in)
        self.root.bind('<Control-equal>', self.zoom_in)
        self.root.bind('<Control-minus>', self.zoom_out)
        self.root.bind('<Command-plus>', self.zoom_in)
        self.root.bind('<Command-equal>', self.zoom_in)
        self.root.bind('<Command-minus>', self.zoom_out)
        self.root.focus_set()
    
    def get_scaled_font_size(self, base_name):
        """Get scaled font size based on current zoom level"""
        return max(8, int(self.base_font_sizes[base_name] * self.font_scale))
    
    def zoom_in(self, event=None):
        """Increase font sizes (zoom in)"""
        if self.font_scale < 2.0:
            self.font_scale += 0.1
            self.update_fonts()
    
    def zoom_out(self, event=None):
        """Decrease font sizes (zoom out)"""
        if self.font_scale > 0.5:
            self.font_scale -= 0.1
            self.update_fonts()
    
    def update_fonts(self):
        """Update all fonts with new scale"""
        # Update title
        self.title_label.configure(font=('Segoe UI', self.get_scaled_font_size('title'), 'bold'))
        self.subtitle_label.configure(font=('Segoe UI', self.get_scaled_font_size('subtitle')))
        
        # Update query text
        self.query_text.configure(font=('JetBrains Mono', self.get_scaled_font_size('query')))
        
        # Update message and error text areas
        self.messages_text.configure(font=('JetBrains Mono', self.get_scaled_font_size('message')))
        self.error_text.configure(font=('JetBrains Mono', self.get_scaled_font_size('message')))
        
        # Update status label
        self.status_label.configure(font=('Segoe UI', self.get_scaled_font_size('label'), 'bold'))
        
        # Update table font if it exists
        if self.tree:
            table_font = ('Segoe UI', self.get_scaled_font_size('table'))
            style = ttk.Style()
            style.configure('Modern.Treeview', font=table_font, rowheight=max(25, int(30 * self.font_scale)))
            style.configure('Modern.Treeview.Heading', font=('Segoe UI', self.get_scaled_font_size('table'), 'bold'))
        
    def create_widgets(self):
        # Main container with modern spacing
        main_frame = ttk.Frame(self.root, padding="30")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(2, weight=1)
        main_frame.rowconfigure(4, weight=2)
        
        # Modern header section
        header_frame = ttk.Frame(main_frame)
        header_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 30))
        header_frame.columnconfigure(0, weight=1)
        
        # Title with modern styling
        self.title_label = ttk.Label(header_frame, 
                               text="SU-SQL Database Engine", 
                               font=('Segoe UI', self.get_scaled_font_size('title'), 'bold'),
                               foreground=self.colors['text_primary'])
        self.title_label.grid(row=0, column=0, sticky=tk.W)
        
        # Subtitle
        self.subtitle_label = ttk.Label(header_frame,
                                  text="Modern SQL Query Interface with Advanced Features",
                                  font=('Segoe UI', self.get_scaled_font_size('subtitle')),
                                  foreground=self.colors['text_secondary'])
        self.subtitle_label.grid(row=1, column=0, sticky=tk.W, pady=(5, 0))
        
        # Status indicator in header
        status_frame = ttk.Frame(header_frame)
        status_frame.grid(row=0, column=1, rowspan=2, sticky=tk.E, padx=(20, 0))
        
        ttk.Label(status_frame, text="Status:", 
                 foreground=self.colors['text_secondary'],
                 font=('Segoe UI', 9)).pack(side=tk.LEFT, padx=(0, 8))
        
        self.status_var = tk.StringVar()
        self.status_var.set("🟢 Ready")
        self.status_label = ttk.Label(status_frame, textvariable=self.status_var, 
                                    foreground=self.colors['accent_primary'], 
                                    font=('Segoe UI', self.get_scaled_font_size('label'), 'bold'))
        self.status_label.pack(side=tk.LEFT)
        
        # Modern query input section
        query_frame = ttk.LabelFrame(main_frame, text="📝 SQL Query Editor", 
                                   padding="25", style='Modern.TLabelFrame')
        query_frame.grid(row=2, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 20))
        query_frame.columnconfigure(0, weight=1)
        query_frame.rowconfigure(0, weight=1)
        
        # Create query text container with modern styling
        query_container = tk.Frame(query_frame, bg=self.colors['bg_secondary'], relief='flat', bd=2)
        query_container.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=5, pady=5)
        query_container.columnconfigure(0, weight=1)
        query_container.rowconfigure(0, weight=1)
        
        # Query text area with modern code editor styling
        self.query_text = scrolledtext.ScrolledText(
            query_container, 
            height=8, 
            font=('JetBrains Mono', self.get_scaled_font_size('query')),
            bg=self.colors['bg_secondary'],
            fg=self.colors['text_primary'],
            insertbackground='#79c0ff',
            selectbackground=self.colors['accent_secondary'],
            selectforeground='white',
            borderwidth=0,
            relief='flat',
            wrap=tk.NONE,
            padx=15,
            pady=15
        )
        self.query_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Modern action buttons section
        actions_frame = ttk.Frame(main_frame)
        actions_frame.grid(row=3, column=0, sticky=(tk.W, tk.E), pady=(0, 25))
        
        # Left side - main actions
        main_actions = ttk.Frame(actions_frame)
        main_actions.pack(side=tk.LEFT)
        
        # Execute button with modern styling
        self.execute_btn = ttk.Button(
            main_actions, 
            text="▶ Execute Query", 
            command=self.execute_query,
            style='Modern.Execute.TButton'
        )
        self.execute_btn.pack(side=tk.LEFT, padx=(0, 15))
        
        # Secondary action buttons
        self.show_tables_btn = ttk.Button(
            main_actions, 
            text="📊 Show Tables", 
            command=self.show_tables,
            style='Modern.Secondary.TButton'
        )
        self.show_tables_btn.pack(side=tk.LEFT, padx=(0, 15))
        
        self.show_schema_btn = ttk.Button(
            main_actions, 
            text="🏗️ Show Schema", 
            command=self.show_schema,
            style='Modern.Secondary.TButton'
        )
        self.show_schema_btn.pack(side=tk.LEFT, padx=(0, 15))
        
        # Right side - utility actions
        utility_actions = ttk.Frame(actions_frame)
        utility_actions.pack(side=tk.RIGHT)
        
        # Zoom controls
        zoom_frame = ttk.Frame(utility_actions)
        zoom_frame.pack(side=tk.LEFT, padx=(0, 15))
        
        ttk.Label(zoom_frame, text="Zoom:", 
                 foreground=self.colors['text_secondary']).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(zoom_frame, text="🔍+", command=self.zoom_in, 
                  style='Modern.Utility.TButton', width=4).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(zoom_frame, text="🔍−", command=self.zoom_out, 
                  style='Modern.Utility.TButton', width=4).pack(side=tk.LEFT)
        
        # Clear button
        self.clear_btn = ttk.Button(
            utility_actions, 
            text="🗑️ Clear", 
            command=self.clear_query,
            style='Modern.Utility.TButton'
        )
        self.clear_btn.pack(side=tk.LEFT)
        
        # Modern results section
        results_frame = ttk.LabelFrame(main_frame, text="📈 Query Results & Analytics", 
                                     padding="20", style='Modern.TLabelFrame')
        results_frame.grid(row=4, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        results_frame.columnconfigure(0, weight=1)
        results_frame.rowconfigure(0, weight=1)
        
        # Create modern notebook for tabs
        self.notebook = ttk.Notebook(results_frame, style='Modern.TNotebook')
        self.notebook.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=5, pady=10)
        
        # Results tab
        self.results_tab = ttk.Frame(self.notebook, padding="15")
        self.notebook.add(self.results_tab, text="📊 Results")
        
        self.results_tab.columnconfigure(0, weight=1)
        self.results_tab.rowconfigure(0, weight=1)
        
        # Treeview frame for results
        self.tree_frame = ttk.Frame(self.results_tab)
        self.tree_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        self.tree_frame.columnconfigure(0, weight=1)
        self.tree_frame.rowconfigure(0, weight=1)
        
        # Initially empty
        self.tree = None
        self.tree_scroll_y = None
        self.tree_scroll_x = None
        
        # Messages tab
        self.messages_tab = ttk.Frame(self.notebook, padding="15")
        self.notebook.add(self.messages_tab, text="💬 Messages")
        
        self.messages_tab.columnconfigure(0, weight=1)
        self.messages_tab.rowconfigure(0, weight=1)
        
        # Create messages container
        messages_container = tk.Frame(self.messages_tab, bg=self.colors['bg_secondary'], relief='flat', bd=2)
        messages_container.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        messages_container.columnconfigure(0, weight=1)
        messages_container.rowconfigure(0, weight=1)
        
        self.messages_text = scrolledtext.ScrolledText(
            messages_container,
            height=12,
            font=('JetBrains Mono', self.get_scaled_font_size('message')),
            bg=self.colors['bg_secondary'],
            fg=self.colors['text_primary'],
            insertbackground='#79c0ff',
            selectbackground=self.colors['accent_secondary'],
            borderwidth=0,
            relief='flat',
            wrap=tk.WORD,
            padx=15,
            pady=15
        )
        self.messages_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Error tab
        self.error_tab = ttk.Frame(self.notebook, padding="15")
        self.notebook.add(self.error_tab, text="⚠️ Errors")
        
        self.error_tab.columnconfigure(0, weight=1)
        self.error_tab.rowconfigure(0, weight=1)
        
        # Create error container
        error_container = tk.Frame(self.error_tab, bg=self.colors['bg_secondary'], relief='flat', bd=2)
        error_container.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        error_container.columnconfigure(0, weight=1)
        error_container.rowconfigure(0, weight=1)
        
        self.error_text = scrolledtext.ScrolledText(
            error_container,
            height=12,
            font=('JetBrains Mono', self.get_scaled_font_size('message')),
            bg=self.colors['bg_secondary'],
            fg=self.colors['accent_danger'],
            insertbackground='#79c0ff',
            selectbackground=self.colors['accent_danger'],
            borderwidth=0,
            relief='flat',
            wrap=tk.WORD,
            padx=15,
            pady=15
        )
        self.error_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Bind keyboard shortcuts
        self.query_text.bind('<Control-Return>', lambda e: self.execute_query())
        self.query_text.bind('<F5>', lambda e: self.execute_query())
        
        # Add modern sample query with syntax
        sample_query = """-- 🚀 SU-SQL Query Examples
-- Basic SELECT query
SELECT * FROM users;

-- Filtered SELECT with conditions  
SELECT name, email FROM users WHERE age > 25;

-- INSERT new data
INSERT INTO users (name, email, age) VALUES ('John Doe', 'john@example.com', 30);

-- Press Ctrl+Enter or F5 to execute queries
-- Use the buttons below for additional actions"""
        
        self.query_text.insert('1.0', sample_query)
        
        # Add syntax highlighting tags
        self.add_syntax_highlighting()
        
    def add_syntax_highlighting(self):
        """Add basic syntax highlighting to the query editor"""
        # SQL Keywords
        keywords = ['SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES', 'UPDATE', 'DELETE', 'CREATE', 'TABLE', 'DROP']
        
        # Configure tags
        self.query_text.tag_configure('keyword', foreground='#ff7b72')
        self.query_text.tag_configure('comment', foreground='#8b949e', font=('JetBrains Mono', self.get_scaled_font_size('query'), 'italic'))
        self.query_text.tag_configure('string', foreground='#a5d6ff')
        
    def show_schema(self):
        """Show database schema information"""
        self.clear_results()
        
        if not database:
            self.show_empty_state("No database schema found", "🏗️")
            return
            
        # Create treeview for schema info
        columns = ['Table', 'Sample Columns', 'Data Types', 'Row Count']
        self.tree = ttk.Treeview(self.tree_frame, columns=columns, show='headings', 
                               height=15, style='Modern.Treeview')
        
        # Configure schema display
        self.configure_treeview(columns, [200, 300, 200, 100])
        
        # Insert schema data
        for i, (table_name, table_rows) in enumerate(database.items()):
            if table_rows:
                sample_cols = ', '.join(list(table_rows[0].keys())[:3])
                if len(table_rows[0].keys()) > 3:
                    sample_cols += '...'
                data_types = 'Mixed'  # You can enhance this based on your data
            else:
                sample_cols = 'No data'
                data_types = 'Unknown'
                
            item_id = self.tree.insert('', tk.END, values=[
                table_name, sample_cols, data_types, len(table_rows)
            ])
            
            if i % 2 == 1:
                self.tree.item(item_id, tags=('oddrow',))
        
        self.tree.tag_configure('oddrow', background=self.colors['bg_secondary'])
        self.log_message(f"Schema displayed for {len(database)} tables.")
        
    def show_empty_state(self, message, icon="📊"):
        """Show a modern empty state message"""
        empty_frame = ttk.Frame(self.tree_frame)
        empty_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        empty_frame.columnconfigure(0, weight=1)
        empty_frame.rowconfigure(0, weight=1)
        
        content_frame = ttk.Frame(empty_frame)
        content_frame.grid(row=0, column=0)
        
        # Large icon
        icon_label = ttk.Label(content_frame, text=icon, font=('Segoe UI', 48),
                             foreground=self.colors['text_secondary'])
        icon_label.pack(pady=(40, 20))
        
        # Message
        msg_label = ttk.Label(content_frame, text=message,
                            font=('Segoe UI', 14),
                            foreground=self.colors['text_secondary'])
        msg_label.pack(pady=(0, 40))
        
    def configure_treeview(self, columns, widths):
        """Configure treeview with modern styling"""
        table_font = ('Segoe UI', self.get_scaled_font_size('table'))
        style = ttk.Style()
        style.configure('Modern.Treeview', font=table_font, rowheight=max(25, int(30 * self.font_scale)))
        style.configure('Modern.Treeview.Heading', font=('Segoe UI', self.get_scaled_font_size('table'), 'bold'))
        
        for i, (col, width) in enumerate(zip(columns, widths)):
            self.tree.heading(col, text=col)
            self.tree.column(col, width=width, minwidth=80)
        
        # Add scrollbars
        self.tree_scroll_y = ttk.Scrollbar(self.tree_frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree_scroll_x = ttk.Scrollbar(self.tree_frame, orient=tk.HORIZONTAL, command=self.tree.xview)
        self.tree.configure(yscrollcommand=self.tree_scroll_y.set, xscrollcommand=self.tree_scroll_x.set)
        
        # Grid layout
        self.tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        self.tree_scroll_y.grid(row=0, column=1, sticky=(tk.N, tk.S))
        self.tree_scroll_x.grid(row=1, column=0, sticky=(tk.W, tk.E))
        
    def log_error(self, message):
        """Add error message to the error tab with modern formatting"""
        import datetime
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        
        formatted_message = f"""╭─ ERROR [{timestamp}]
│ {message}
╰─────────────────────────────────────

"""
        
        self.error_text.insert(tk.END, formatted_message)
        self.error_text.see(tk.END)
        self.notebook.select(2)
        
    def log_message(self, message, level="INFO"):
        """Add message to the messages tab with modern formatting"""
        import datetime
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        
        icons = {"INFO": "ℹ️", "SUCCESS": "✅", "WARNING": "⚠️"}
        icon = icons.get(level, "ℹ️")
        
        formatted_message = f"""╭─ {icon} {level} [{timestamp}]
│ {message}
╰─────────────────────────────────────

"""
        
        self.messages_text.insert(tk.END, formatted_message)
        self.messages_text.see(tk.END)
        
    def clear_results(self):
        """Clear the results display"""
        if self.tree:
            self.tree.destroy()
            self.tree = None
        if self.tree_scroll_y:
            self.tree_scroll_y.destroy()
            self.tree_scroll_y = None
        if self.tree_scroll_x:
            self.tree_scroll_x.destroy()
            self.tree_scroll_x = None
    
    def display_table_results(self, rows):
        """Display query results with modern styling"""
        self.clear_results()
        
        if not rows:
            self.show_empty_state("Query executed successfully\nNo results to display", "✅")
            return
            
        # Get columns from first row
        columns = list(rows[0].keys())
        
        # Create modern treeview
        self.tree = ttk.Treeview(self.tree_frame, columns=columns, show='headings', 
                               height=15, style='Modern.Treeview')
        
        # Calculate optimal widths
        widths = []
        for col in columns:
            col_content_width = max(len(str(col)), max(len(str(row.get(col, ''))) for row in rows))
            optimal_width = min(col_content_width * 12 + 50, 300)
            widths.append(optimal_width)
        
        self.configure_treeview(columns, widths)
        
        # Insert data with modern styling
        for i, row in enumerate(rows):
            values = [str(row.get(col, '')) for col in columns]
            item_id = self.tree.insert('', tk.END, values=values)
            if i % 2 == 1:
                self.tree.item(item_id, tags=('oddrow',))
        
        self.tree.tag_configure('oddrow', background=self.colors['bg_secondary'])
        self.log_message(f"Query executed successfully. {len(rows)} rows returned.", "SUCCESS")
        
    def display_tables_info(self):
        """Display tables information with modern styling"""
        self.clear_results()
        
        if not database:
            self.show_empty_state("No tables found in database", "📊")
            return
            
        columns = ['Table Name', 'Total Rows', 'Status', 'Last Modified']
        self.tree = ttk.Treeview(self.tree_frame, columns=columns, show='headings', 
                               height=15, style='Modern.Treeview')
        
        self.configure_treeview(columns, [250, 120, 120, 150])
        
        # Insert table data with enhanced info
        import datetime
        for i, (table_name, table_rows) in enumerate(database.items()):
            row_count = len(table_rows)
            if row_count > 0:
                status = "🟢 Active"
            else:
                status = "🔴 Empty"
            
            # Mock last modified (you can enhance this with real data)
            last_modified = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
            
            item_id = self.tree.insert('', tk.END, values=[table_name, row_count, status, last_modified])
            if i % 2 == 1:
                self.tree.item(item_id, tags=('oddrow',))
        
        self.tree.tag_configure('oddrow', background=self.colors['bg_secondary'])
        self.log_message(f"Found {len(database)} tables in database.", "SUCCESS")
    
    def execute_query(self):
        """Execute the SU-SQL query with modern feedback"""
        query = self.query_text.get('1.0', tk.END).strip()
        
        if not query or all(line.strip().startswith('--') or not line.strip() for line in query.split('\n')):
            self.log_error("Please enter a valid SU-SQL query.")
            return
            
        try:
            self.status_var.set("🔄 Executing...")
            self.root.update()
            
            # Clean the query (remove comments)
            clean_query = '\n'.join([line for line in query.split('\n') 
                                   if not line.strip().startswith('--') and line.strip()])
            
            if not clean_query.strip():
                self.log_error("Please enter a valid SU-SQL query.")
                return
            
            lexer = Lexer(clean_query)
            parser = Parser(lexer.tokens)
            
            if not lexer.tokens:
                self.log_error("Empty query after removing comments.")
                return
            
            first_token = lexer.tokens[0][0].upper()
            
            if first_token == "SELECT":
                ast = parser.parse_select_statement()
                rows = execute(ast, database)
                self.display_table_results(rows)
                self.notebook.select(0)  # Switch to results tab
                
            elif first_token == "INSERT":
                ast = parser.parse_insert_statement()
                execute(ast, database)
                self.show_success_message("INSERT executed successfully", 
                                        "Data has been added to the database")
                self.notebook.select(0)  # Switch to results tab
                
            elif first_token == "UPDATE":
                self.log_error("UPDATE statement support coming soon!")
                
            else:
                self.log_error(f"Unsupported query type: {first_token}")
                return
                
            self.status_var.set("🟢 Query completed")
            
        except KeyError as k:
            error_msg = f"Row/column {k} not found"
            self.log_error(error_msg)
            self.status_var.set("🔴 Error")
            
        except Exception as e:
            error_msg = f"Query error: {str(e)}"
            self.log_error(error_msg)
            self.status_var.set("🔴 Error")
    
    def show_success_message(self, title, message):
        """Show a modern success message"""
        self.clear_results()
        
        success_frame = ttk.Frame(self.tree_frame)
        success_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        success_frame.columnconfigure(0, weight=1)
        success_frame.rowconfigure(0, weight=1)
        
        content_frame = ttk.Frame(success_frame)
        content_frame.grid(row=0, column=0)
        
        # Success icon
        icon_label = ttk.Label(content_frame, text="✅", font=('Segoe UI', 64),
                             foreground=self.colors['accent_primary'])
        icon_label.pack(pady=(40, 20))
        
        # Title
        title_label = ttk.Label(content_frame, text=title,
                              font=('Segoe UI', 16, 'bold'),
                              foreground=self.colors['text_primary'])
        title_label.pack(pady=(0, 10))
        
        # Message
        msg_label = ttk.Label(content_frame, text=message,
                            font=('Segoe UI', 12),
                            foreground=self.colors['text_secondary'])
        msg_label.pack(pady=(0, 40))
        
        self.log_message(f"{title}. {message}", "SUCCESS")
    
    def show_tables(self):
        """Show all tables in the database"""
        self.display_tables_info()
        self.notebook.select(0)  # Switch to results tab
        self.status_var.set("🟢 Tables displayed")
    
    def clear_query(self):
        """Clear the query text area"""
        self.query_text.delete('1.0', tk.END)
        self.status_var.set("🟢 Query cleared")

def main():
    root = tk.Tk()
    
    # Set minimum window size for modern layout
    root.minsize(1200, 800)
    
    app = DatabaseGUI(root)
    
    # Center the window on screen
    root.update_idletasks()
    width = root.winfo_width()
    height = root.winfo_height()
    x = (root.winfo_screenwidth() // 2) - (width // 2)
    y = (root.winfo_screenheight() // 2) - (height // 2)
    root.geometry(f"{width}x{height}+{x}+{y}")
    
    # Make window responsive
    root.columnconfigure(0, weight=1)
    root.rowconfigure(0, weight=1)
    
    root.mainloop()

if __name__ == "__main__":
    main()