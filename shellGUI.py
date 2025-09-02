import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import sys
import os
import re
from database import database
from engine import Lexer, Parser
from executor import execute

class LineNumberedText:
    """Text widget with line numbers like a real IDE"""
    def __init__(self, parent, **kwargs):
        self.frame = tk.Frame(parent)
        self.font = kwargs.get('font', ('JetBrains Mono', 12))
        
        # Line numbers text widget
        self.line_numbers = tk.Text(
            self.frame,
            width=4,
            padx=3,
            pady=15,  # Match padding with text widget
            takefocus=0,
            border=0,
            state='disabled',
            wrap='none',
            font=self.font,
            bg='#1e1e1e',
            fg='#858585',
            selectbackground='#264f78',
            spacing1=0,  # Ensure line spacing matches text widget
            spacing2=0,
            spacing3=0
        )
        self.line_numbers.pack(side=tk.LEFT, fill=tk.Y)
        
        # Main text widget
        self.text = scrolledtext.ScrolledText(
            self.frame,
            wrap=tk.NONE,
            font=self.font,
            bg='#1e1e1e',
            fg='#d4d4d4',
            insertbackground='#ffffff',
            selectbackground='#264f78',
            selectforeground='#ffffff',
            borderwidth=0,
            relief='flat',
            padx=15,
            pady=15,
            undo=True,
            maxundo=20,
            spacing1=0,  # Consistent line spacing
            spacing2=0,
            spacing3=0
        )
        self.text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # Bind events for line number updates
        self.text.bind('<KeyRelease>', self.on_content_changed)
        self.text.bind('<Button-1>', self.on_content_changed)
        self.text.bind('<Control-a>', self.select_all)
        
        # Initial line numbers
        self.update_line_numbers()
        self.configure_syntax_highlighting()
    
    def on_content_changed(self, event=None):
        """Update line numbers when content changes"""
        self.update_line_numbers()
        self.apply_syntax_highlighting()
    
    def select_all(self, event=None):
        """Select all text"""
        self.text.tag_add(tk.SEL, "1.0", tk.END)
        return "break"
    
    def update_line_numbers(self):
        """Update the line numbers display"""
        self.line_numbers.config(state='normal')
        self.line_numbers.delete('1.0', tk.END)
        
        line_count = int(self.text.index('end-1c').split('.')[0])
        line_numbers_content = '\n'.join(str(i) for i in range(1, line_count + 1))
        
        self.line_numbers.insert('1.0', line_numbers_content)
        self.line_numbers.config(state='disabled')
    
    def configure_syntax_highlighting(self):
        """Configure SQL syntax highlighting tags"""
        font_base = (self.font[0], self.font[1]) if len(self.font) > 1 else (self.font[0], 12)
        self.text.tag_configure('keyword', foreground='#569cd6', font=font_base + ('bold',))
        self.text.tag_configure('string', foreground='#ce9178', font=font_base)
        self.text.tag_configure('comment', foreground='#6a9955', font=font_base + ('italic',))
        self.text.tag_configure('number', foreground='#b5cea8', font=font_base)
    
    def apply_syntax_highlighting(self):
        """Apply syntax highlighting to the current content"""
        content = self.text.get('1.0', tk.END)
        
        # Remove existing tags
        for tag in ['keyword', 'string', 'comment', 'number']:
            self.text.tag_remove(tag, '1.0', tk.END)
        
        # SQL Keywords
        keywords = ['SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET', 'DELETE', 'CREATE', 'TABLE', 'DROP', 'AND', 'OR', 'NOT']
        
        lines = content.split('\n')
        for line_num, line in enumerate(lines, 1):
            # Highlight comments
            if '--' in line:
                comment_start = line.find('--')
                start_pos = f"{line_num}.{comment_start}"
                end_pos = f"{line_num}.{len(line)}"
                self.text.tag_add('comment', start_pos, end_pos)
            
            # Highlight strings
            for match in re.finditer(r"'[^']*'", line):
                start_pos = f"{line_num}.{match.start()}"
                end_pos = f"{line_num}.{match.end()}"
                self.text.tag_add('string', start_pos, end_pos)
            
            # Highlight numbers
            for match in re.finditer(r'\b\d+\.?\d*\b', line):
                start_pos = f"{line_num}.{match.start()}"
                end_pos = f"{line_num}.{match.end()}"
                self.text.tag_add('number', start_pos, end_pos)
            
            # Highlight keywords
            for keyword in keywords:
                pattern = r'\b' + re.escape(keyword) + r'\b'
                for match in re.finditer(pattern, line, re.IGNORECASE):
                    start_pos = f"{line_num}.{match.start()}"
                    end_pos = f"{line_num}.{match.end()}"
                    self.text.tag_add('keyword', start_pos, end_pos)
    
    def get(self, start, end=None):
        return self.text.get(start, end)
    
    def insert(self, index, text):
        self.text.insert(index, text)
        self.update_line_numbers()
        self.apply_syntax_highlighting()
    
    def delete(self, start, end=None):
        self.text.delete(start, end)
        self.update_line_numbers()
        self.apply_syntax_highlighting()
    
    def bind(self, sequence, func, add=None):
        return self.text.bind(sequence, func, add)
    
    def configure(self, **kwargs):
        if 'font' in kwargs:
            self.font = kwargs['font']
            self.text.configure(font=self.font)
            self.line_numbers.configure(font=self.font)
            self.configure_syntax_highlighting()  # Reconfigure tags with new font
        return self.text.configure(**kwargs)

class DatabaseGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("SU-SQL - Database Engine Interface")
        self.root.geometry("1400x900")
        self.root.configure(bg='#1e1e1e')  # VS Code dark theme
        
        # Set application icon
        self.set_app_icon()
        
        # Font scaling for zoom functionality
        self.font_scale = 1.0
        self.base_font_sizes = {
            'title': 18,
            'subtitle': 11,
            'query': 12,
            'message': 11,
            'button': 10,
            'label': 10,
            'table': 10
        }
        
        # Modern IDE color scheme (VS Code inspired)
        self.colors = {
            'bg_primary': '#1e1e1e',      # Main background
            'bg_secondary': '#252526',     # Secondary background
            'bg_tertiary': '#2d2d30',      # Cards/panels
            'accent_primary': '#0e639c',   # Blue accent
            'accent_secondary': '#1f6feb', # Light blue
            'accent_danger': '#f14c4c',    # Red accent
            'accent_warning': '#ffcc02',   # Yellow accent
            'accent_success': '#00bc7d',   # Green accent
            'text_primary': '#cccccc',     # Primary text
            'text_secondary': '#969696',   # Secondary text
            'border': '#464647',           # Borders
            'hover': '#2a2d2e',            # Hover states
            'selection': '#264f78'         # Selection
        }
        
        # Configure modern theme
        self.setup_modern_theme()
        self.results_minimized = False  # Track if results section is minimized
        self.tables_maximized = False   # Track if tables are maximized (query hidden)
        
        self.create_widgets()
        self.setup_zoom_bindings()
        
    def set_app_icon(self):
        """Set application icon - looks for icon.ico, icon.png, or logo.png in the same directory"""
        icon_files = ['icon.ico', 'icon.png', 'logo.png', 'app_icon.ico', 'app_icon.png']
        script_dir = os.path.dirname(os.path.abspath(__file__))
        for icon_file in icon_files:
            icon_path = os.path.join(script_dir, icon_file)
            if os.path.exists(icon_path):
                try:
                    if icon_path.endswith('.ico'):
                        self.root.iconbitmap(icon_path)
                    else:
                        icon_image = tk.PhotoImage(file=icon_path)
                        self.root.iconphoto(True, icon_image)
                    print(f"Icon loaded: {icon_path}")
                    return
                except Exception as e:
                    print(f"Failed to load icon {icon_path}: {e}")
                    continue
    
    def setup_modern_theme(self):
        """Configure modern dark theme"""
        style = ttk.Style()
        style.theme_use('clam')

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
        
        # Modern LabelFrame
        style.configure('Modern.TLabelFrame', 
                       background=self.colors['bg_tertiary'], 
                       foreground=self.colors['text_primary'],
                       borderwidth=1,
                       relief='solid')
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
                       padding=(25, 12),
                       font=('Segoe UI', 10, 'bold'))
        
        # Modern secondary buttons
        style.configure('Modern.Secondary.TButton',
                       background=self.colors['bg_tertiary'],
                       foreground=self.colors['text_primary'],
                       borderwidth=1,
                       bordercolor=self.colors['border'],
                       focuscolor='none',
                       padding=(20, 10),
                       font=('Segoe UI', 9))
        
        # Modern utility buttons
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
                       background=self.colors['bg_primary'], 
                       borderwidth=0)
        
        style.configure('Modern.TNotebook.Tab', 
                       background=self.colors['bg_secondary'], 
                       foreground=self.colors['text_secondary'], 
                       padding=[15, 10],
                       font=('Segoe UI', 9),
                       borderwidth=0)
        
        # Modern treeview
        style.configure('Modern.Treeview', 
                       background=self.colors['bg_tertiary'], 
                       foreground=self.colors['text_primary'], 
                       fieldbackground=self.colors['bg_tertiary'], 
                       borderwidth=0,
                       rowheight=28)
        
        style.configure('Modern.Treeview.Heading', 
                       background=self.colors['bg_secondary'], 
                       foreground=self.colors['text_primary'],
                       borderwidth=0,
                       font=('Segoe UI', 10, 'bold'))
        
        # Modern hover effects
        style.map('Modern.Execute.TButton', 
                 background=[('active', '#1177bb'), ('pressed', '#0e639c')])
        
        style.map('Modern.Secondary.TButton', 
                 background=[('active', self.colors['hover']), 
                           ('pressed', self.colors['bg_secondary'])])
        
        style.map('Modern.Utility.TButton',
                 background=[('active', self.colors['hover']),
                           ('pressed', self.colors['bg_primary'])])
        
        style.map('Modern.TNotebook.Tab', 
                 background=[('selected', self.colors['bg_primary']), 
                           ('active', self.colors['hover'])],
                 foreground=[('selected', self.colors['text_primary'])])
        
        style.map('Modern.Treeview', 
                 background=[('selected', self.colors['selection'])],
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
        self.title_label.configure(font=('Segoe UI', self.get_scaled_font_size('title'), 'bold'))
        self.subtitle_label.configure(font=('Segoe UI', self.get_scaled_font_size('subtitle')))
        query_font = ('JetBrains Mono', self.get_scaled_font_size('query'))
        self.query_text.configure(font=query_font)
        self.messages_text.configure(font=('JetBrains Mono', self.get_scaled_font_size('message')))
        self.status_label.configure(font=('Segoe UI', self.get_scaled_font_size('label'), 'bold'))
        if self.tree:
            table_font = ('Segoe UI', self.get_scaled_font_size('table'))
            style = ttk.Style()
            style.configure('Modern.Treeview', font=table_font, rowheight=max(25, int(28 * self.font_scale)))
            style.configure('Modern.Treeview.Heading', font=('Segoe UI', self.get_scaled_font_size('table'), 'bold'))
            # Adjust column widths based on new font size if data exists
            if hasattr(self, 'current_columns') and self.current_columns:
                for col, width in zip(self.current_columns, self.current_widths):
                    self.tree.column(col, width=int(width * self.font_scale))
        
    def toggle_results_view(self):
        """Toggle the visibility of the results section"""
        if self.results_minimized:
            # Maximize (show) results section
            self.results_frame.grid(row=4, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
            self.main_frame.rowconfigure(4, weight=1)  # Restore weight
            self.toggle_results_btn.configure(text="Minimize Results ↓")
            self.results_minimized = False
        else:
            # Minimize (hide) results section
            self.results_frame.grid_forget()
            self.main_frame.rowconfigure(4, weight=0)  # Remove weight
            self.toggle_results_btn.configure(text="Maximize Results ↑")
            self.results_minimized = True

    def toggle_tables_fullscreen(self):
        """Toggle fullscreen view of tables (hide query editor and other UI elements)"""
        if self.tables_maximized:
            # Restore default view (show query editor and other elements)
            self.control_frame.grid_forget()  # Hide control frame used in maximized view
            self.header_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 20))
            self.query_frame.grid(row=2, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 15))
            self.actions_frame.grid(row=3, column=0, sticky=(tk.W, tk.E), pady=(0, 15))
            if not self.results_minimized:
                self.results_frame.grid(row=4, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
            self.main_frame.rowconfigure(2, weight=3)  # Query editor weight
            self.main_frame.rowconfigure(4, weight=1)  # Results weight
            self.toggle_tables_btn.configure(text="Maximize Tables")
            self.tables_maximized = False
        else:
            # Hide query editor and other elements, maximize tables
            self.header_frame.grid_forget()
            self.query_frame.grid_forget()
            self.actions_frame.grid_forget()
            if self.results_minimized:
                self.results_frame.grid(row=4, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
                self.results_minimized = False
                self.toggle_results_btn.configure(text="Minimize Results ↓")
            self.main_frame.rowconfigure(2, weight=0)  # Hide query weight
            self.main_frame.rowconfigure(4, weight=1)  # Full weight to results
            
            # Create a small control frame for the restore button in maximized view
            self.control_frame = ttk.Frame(self.main_frame)
            self.control_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(5, 5))
            self.restore_btn = ttk.Button(
                self.control_frame, 
                text="Restore View", 
                command=self.toggle_tables_fullscreen,
                style='Modern.Utility.TButton'
            )
            self.restore_btn.pack(side=tk.RIGHT, padx=10)
            
            self.toggle_tables_btn.configure(text="Restore View")
            self.tables_maximized = True

    def create_widgets(self):
        # Main container
        self.main_frame = ttk.Frame(self.root, padding="20")
        self.main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        self.main_frame.columnconfigure(0, weight=1)
        self.main_frame.rowconfigure(2, weight=3)  # Give more weight to query editor
        self.main_frame.rowconfigure(4, weight=1)  # Less weight to results
        
        # Header section
        self.header_frame = ttk.Frame(self.main_frame)
        self.header_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 20))
        self.header_frame.columnconfigure(0, weight=1)
        
        # Title
        self.title_label = ttk.Label(self.header_frame, 
                               text="SU-SQL Database Engine", 
                               font=('Segoe UI', self.get_scaled_font_size('title'), 'bold'),
                               foreground=self.colors['text_primary'])
        self.title_label.grid(row=0, column=0, sticky=tk.W)
        
        # Subtitle
        self.subtitle_label = ttk.Label(self.header_frame,
                                  text="Professional SQL Query Interface",
                                  font=('Segoe UI', self.get_scaled_font_size('subtitle')),
                                  foreground=self.colors['text_secondary'])
        self.subtitle_label.grid(row=1, column=0, sticky=tk.W, pady=(5, 0))
        
        # Status indicator
        status_frame = ttk.Frame(self.header_frame)
        status_frame.grid(row=0, column=1, rowspan=2, sticky=tk.E, padx=(20, 0))
        
        ttk.Label(status_frame, text="Status:", 
                 foreground=self.colors['text_secondary'],
                 font=('Segoe UI', 9)).pack(side=tk.LEFT, padx=(0, 8))
        
        self.status_var = tk.StringVar()
        self.status_var.set("Ready")
        self.status_label = ttk.Label(status_frame, textvariable=self.status_var, 
                                    foreground=self.colors['accent_success'], 
                                    font=('Segoe UI', self.get_scaled_font_size('label'), 'bold'))
        self.status_label.pack(side=tk.LEFT)
        
        # Query input section
        self.query_frame = ttk.LabelFrame(self.main_frame, text="SQL Query Editor", 
                                   padding="20", style='Modern.TLabelFrame')
        self.query_frame.grid(row=2, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 15))
        self.query_frame.columnconfigure(0, weight=1)
        self.query_frame.rowconfigure(0, weight=1)
        
        # Line numbered query editor
        self.query_text = LineNumberedText(
            self.query_frame, 
            font=('JetBrains Mono', self.get_scaled_font_size('query')),
            height=8
        )
        self.query_text.frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=5, pady=5)
        
        # Action buttons section
        self.actions_frame = ttk.Frame(self.main_frame)
        self.actions_frame.grid(row=3, column=0, sticky=(tk.W, tk.E), pady=(0, 15))
        
        # Left side - main actions
        main_actions = ttk.Frame(self.actions_frame)
        main_actions.pack(side=tk.LEFT)
        
        # Execute button
        self.execute_btn = ttk.Button(
            main_actions, 
            text="Execute Query", 
            command=self.execute_queries,
            style='Modern.Execute.TButton'
        )
        self.execute_btn.pack(side=tk.LEFT, padx=(0, 15))
        
        # Secondary buttons
        self.show_tables_btn = ttk.Button(
            main_actions, 
            text="Show Tables", 
            command=self.show_tables,
            style='Modern.Secondary.TButton'
        )
        self.show_tables_btn.pack(side=tk.LEFT, padx=(0, 15))
        
        self.show_schema_btn = ttk.Button(
            main_actions, 
            text="Show Schema", 
            command=self.show_schema,
            style='Modern.Secondary.TButton'
        )
        self.show_schema_btn.pack(side=tk.LEFT, padx=(0, 15))
        
        # Right side - utility actions
        utility_actions = ttk.Frame(self.actions_frame)
        utility_actions.pack(side=tk.RIGHT)
        
        # Toggle tables fullscreen button
        self.toggle_tables_btn = ttk.Button(
            utility_actions, 
            text="Maximize Tables", 
            command=self.toggle_tables_fullscreen,
            style='Modern.Utility.TButton'
        )
        self.toggle_tables_btn.pack(side=tk.LEFT, padx=(0, 15))
        
        # Toggle results view button
        self.toggle_results_btn = ttk.Button(
            utility_actions, 
            text="Minimize Results ↓", 
            command=self.toggle_results_view,
            style='Modern.Utility.TButton'
        )
        self.toggle_results_btn.pack(side=tk.LEFT, padx=(0, 15))
        
        # Zoom controls
        zoom_frame = ttk.Frame(utility_actions)
        zoom_frame.pack(side=tk.LEFT, padx=(0, 15))
        
        ttk.Label(zoom_frame, text="Zoom:", 
                 foreground=self.colors['text_secondary']).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(zoom_frame, text="+", command=self.zoom_in, 
                  style='Modern.Utility.TButton', width=3).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(zoom_frame, text="-", command=self.zoom_out, 
                  style='Modern.Utility.TButton', width=3).pack(side=tk.LEFT)
        
        # Clear button
        self.clear_btn = ttk.Button(
            utility_actions, 
            text="Clear", 
            command=self.clear_query,
            style='Modern.Utility.TButton'
        )
        self.clear_btn.pack(side=tk.LEFT)
        
        # Results section
        self.results_frame = ttk.LabelFrame(self.main_frame, text="Query Results", 
                                     padding="15", style='Modern.TLabelFrame')
        self.results_frame.grid(row=4, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        self.results_frame.columnconfigure(0, weight=1)
        self.results_frame.rowconfigure(0, weight=1)
        
        # Create notebook for tabs
        self.notebook = ttk.Notebook(self.results_frame, style='Modern.TNotebook')
        self.notebook.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=5, pady=5)
        
        # Results tab
        self.results_tab = ttk.Frame(self.notebook, padding="10")
        self.notebook.add(self.results_tab, text="Results")
        
        self.results_tab.columnconfigure(0, weight=1)
        self.results_tab.rowconfigure(1, weight=1)
        
        # Treeview frame for results
        self.tree_frame = ttk.Frame(self.results_tab)
        self.tree_frame.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        self.tree_frame.columnconfigure(0, weight=1)
        self.tree_frame.rowconfigure(0, weight=1)
        
        # Initially empty
        self.tree = None
        self.tree_scroll_y = None
        self.tree_scroll_x = None
        self.current_columns = []  # Store current columns for zoom adjustment
        self.current_widths = []   # Store base widths for zoom adjustment
        
        # Messages tab
        self.messages_tab = ttk.Frame(self.notebook, padding="10")
        self.notebook.add(self.messages_tab, text="Messages")
        
        self.messages_tab.columnconfigure(0, weight=1)
        self.messages_tab.rowconfigure(0, weight=1)
        
        # Create messages container
        messages_container = tk.Frame(self.messages_tab, bg=self.colors['bg_secondary'], relief='solid', bd=1)
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
        
        # Bind keyboard shortcuts
        self.query_text.bind('<Control-Return>', lambda e: self.execute_queries())
        self.query_text.bind('<F5>', lambda e: self.execute_queries())
        
        # Add sample query
        sample_query = """-- SU-SQL Query Examples
-- Basic SELECT query
SELECT * FROM users;

-- Filtered SELECT with conditions  
SELECT name, email FROM users WHERE age > 25;

-- INSERT new data
INSERT INTO users (name, email, age) VALUES ('John Doe', 'john@example.com', 30);

-- Multiple queries (separate with semicolons)
INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com');
SELECT * FROM users;"""
        
        self.query_text.insert('1.0', sample_query)
        
    def show_schema(self):
        """Show database schema information"""
        self.clear_results()
        
        if not database:
            self.show_empty_state("No database schema found")
            return
            
        # Create treeview for schema info
        columns = ['Table', 'Sample Columns', 'Data Types', 'Row Count']
        self.tree = ttk.Treeview(self.tree_frame, columns=columns, show='headings', 
                               height=15, style='Modern.Treeview')
        
        # Configure schema display
        self.configure_treeview(columns, [200, 300, 200, 100])
        self.current_columns = columns
        self.current_widths = [200, 300, 200, 100]
        
        # Insert schema data
        for i, (table_name, table_rows) in enumerate(database.items()):
            if table_rows:
                sample_cols = ', '.join(list(table_rows[0].keys())[:3])
                if len(table_rows[0].keys()) > 3:
                    sample_cols += '...'
                data_types = 'Mixed'
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
        
    def show_empty_state(self, message):
        """Show empty state message"""
        empty_frame = ttk.Frame(self.tree_frame)
        empty_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        empty_frame.columnconfigure(0, weight=1)
        empty_frame.rowconfigure(0, weight=1)
        
        content_frame = ttk.Frame(empty_frame)
        content_frame.grid(row=0, column=0)
        
        # Message
        msg_label = ttk.Label(content_frame, text=message,
                            font=('Segoe UI', 14),
                            foreground=self.colors['text_secondary'])
        msg_label.pack(pady=40)
        
    def configure_treeview(self, columns, widths):
        """Configure treeview with modern styling"""
        table_font = ('Segoe UI', self.get_scaled_font_size('table'))
        style = ttk.Style()
        style.configure('Modern.Treeview', font=table_font, rowheight=max(25, int(28 * self.font_scale)))
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
        """Show error message as an alert dialog"""
        messagebox.showerror("Error", message)
        
    def log_message(self, message, level="INFO"):
        """Add message to the messages tab"""
        import datetime
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        formatted_message = f"[{timestamp}] {level}: {message}\n"
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
        self.current_columns = []
        self.current_widths = []
    
    def display_table_results(self, rows):
        """Display query results with modern styling"""
        self.clear_results()
        
        if not rows:
            self.show_empty_state("Query executed successfully\nNo results to display")
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
        self.current_columns = columns
        self.current_widths = widths
        
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
            self.show_empty_state("No tables found in database")
            return
            
        columns = ['Table Name', 'Total Rows', 'Status', 'Last Modified']
        self.tree = ttk.Treeview(self.tree_frame, columns=columns, show='headings', 
                               height=15, style='Modern.Treeview')
        
        self.configure_treeview(columns, [250, 120, 120, 150])
        self.current_columns = columns
        self.current_widths = [250, 120, 120, 150]
        
        # Insert table data
        import datetime
        for i, (table_name, table_rows) in enumerate(database.items()):
            row_count = len(table_rows)
            status = "Active" if row_count > 0 else "Empty"
            last_modified = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
            
            item_id = self.tree.insert('', tk.END, values=[table_name, row_count, status, last_modified])
            if i % 2 == 1:
                self.tree.item(item_id, tags=('oddrow',))
        
        self.tree.tag_configure('oddrow', background=self.colors['bg_secondary'])
        self.log_message(f"Found {len(database)} tables in database.", "SUCCESS")

    def execute_queries(self):
        """Execute multiple queries separated by semicolons, ensuring each query ends with a semicolon"""
        queries = self.query_text.get('1.0', tk.END).strip().split(';')
        self.status_var.set("Executing...")
        self.root.update()

        last_select_result = None

        for query in queries:
            query = query.strip()
            if not query or query.startswith('--'):
                continue
            # Re-append semicolon to ensure the query matches the expected format for the parser
            query_with_semicolon = query + ';'
            try:
                lexer = Lexer(query_with_semicolon)
                if not lexer.tokens:
                    continue
                parser = Parser(lexer.tokens)
                first_token = lexer.tokens[0][0].upper()

                if first_token == "SELECT":
                    ast = parser.parse_select_statement()
                    rows = execute(ast, database)
                    last_select_result = rows
                elif first_token == "INSERT":
                    ast = parser.parse_insert_statement()
                    execute(ast, database)
                    self.log_message("INSERT query executed successfully.", "SUCCESS")
                elif first_token == "UPDATE":
                    ast = parser.parse_update_statement()
                    execute(ast, database)
                    self.log_message("UPDATE query executed successfully.", "SUCCESS")
                else:
                    self.log_error(f"Unsupported query type: {first_token}")
            except KeyError as k:
                self.log_error(f"Row/column {k} not found")
                self.status_var.set("Error")
            except Exception as e:
                self.log_error(f"Query error: {str(e)}")
                self.status_var.set("Error")

        if last_select_result is not None:
            self.display_table_results(last_select_result)
            self.notebook.select(0)
        elif last_select_result is None and self.status_var.get() != "Error":
            self.show_success_message("Queries executed successfully", "All operations completed successfully")
            self.notebook.select(0)

        if self.status_var.get() != "Error":
            self.status_var.set("Execution Complete")
    
    def show_success_message(self, title, message):
        """Show a success message"""
        self.clear_results()
        
        success_frame = ttk.Frame(self.tree_frame)
        success_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        success_frame.columnconfigure(0, weight=1)
        success_frame.rowconfigure(0, weight=1)
        
        content_frame = ttk.Frame(success_frame)
        content_frame.grid(row=0, column=0)
        
        # Title
        title_label = ttk.Label(content_frame, text=title,
                              font=('Segoe UI', 16, 'bold'),
                              foreground=self.colors['accent_success'])
        title_label.pack(pady=(40, 10))
        
        # Message
        msg_label = ttk.Label(content_frame, text=message,
                            font=('Segoe UI', 12),
                            foreground=self.colors['text_secondary'])
        msg_label.pack(pady=(0, 40))
        
        self.log_message(f"{title}. {message}", "SUCCESS")
    
    def show_tables(self):
        """Show all tables in the database"""
        self.display_tables_info()
        self.notebook.select(0)
        self.status_var.set("Tables displayed")
    
    def clear_query(self):
        """Clear the query text area"""
        self.query_text.delete('1.0', tk.END)
        self.status_var.set("Query cleared")

def main():
    root = tk.Tk()
    root.minsize(1200, 800)
    
    app = DatabaseGUI(root)
    
    # Center the window on screen
    root.update_idletasks()
    width = root.winfo_width()
    height = root.winfo_height()
    x = (root.winfo_screenwidth() // 2) - (width // 2)
    y = (root.winfo_screenheight() // 2) - (height // 2)
    root.geometry(f"{width}x{height}+{x}+{y}")
    
    root.columnconfigure(0, weight=1)
    root.rowconfigure(0, weight=1)
    
    root.mainloop()

if __name__ == "__main__":
    main()