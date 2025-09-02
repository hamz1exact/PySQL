import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import sys
from database import database
from engine import Lexer, Parser
from executor import execute

class DatabaseGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("SU-SQL - Database Engine Interface")
        self.root.geometry("1000x700")
        self.root.configure(bg='#1e1e1e')
        
        # Font scaling for zoom functionality
        self.font_scale = 1.0
        self.base_font_sizes = {
            'title': 16,
            'query': 11,
            'message': 10,
            'button': 9,
            'label': 9,
            'table': 9
        }
        
        # Configure dark theme
        self.setup_dark_theme()
        
        self.create_widgets()
        self.setup_zoom_bindings()
        
    def setup_dark_theme(self):
        """Configure dark theme for the application"""
        style = ttk.Style()
        style.theme_use('clam')
        
        # Configure dark colors
        style.configure('TLabel', background='#1e1e1e', foreground='#ffffff')
        style.configure('TFrame', background='#1e1e1e')
        style.configure('TLabelFrame', background='#1e1e1e', foreground='#ffffff')
        style.configure('TLabelFrame.Label', background='#1e1e1e', foreground='#ffffff')
        
        # Enhanced button styling
        style.configure('TButton', 
                       background='#3c3c3c', 
                       foreground='#ffffff',
                       borderwidth=1,
                       focuscolor='#404040',
                       padding=(12, 8))
        
        # Special styling for execute button
        style.configure('Execute.TButton',
                       background='#00ff88',
                       foreground='#000000',
                       borderwidth=1,
                       focuscolor='#00cc66',
                       padding=(15, 10))
        
        style.configure('TNotebook', background='#2d2d2d', foreground='#ffffff')
        style.configure('TNotebook.Tab', background='#3c3c3c', foreground='#ffffff', padding=[12, 8])
        style.configure('Treeview', background='#2d2d2d', foreground='#ffffff', 
                       fieldbackground='#2d2d2d', borderwidth=0)
        style.configure('Treeview.Heading', background='#404040', foreground='#ffffff')
        
        # Enhanced hover effects
        style.map('TButton', 
                 background=[('active', '#4a4a4a'), ('pressed', '#2a2a2a')])
        style.map('Execute.TButton',
                 background=[('active', '#00cc66'), ('pressed', '#00aa55')])
        style.map('TNotebook.Tab', background=[('selected', '#404040')])
        style.map('Treeview', background=[('selected', '#404040')])
        
    def setup_zoom_bindings(self):
        """Setup keyboard shortcuts for zoom functionality"""
        # Bind Cmd+Plus and Cmd+Minus (Mac) or Ctrl+Plus and Ctrl+Minus (Windows/Linux)
        self.root.bind('<Control-plus>', self.zoom_in)
        self.root.bind('<Control-equal>', self.zoom_in)  # For keyboards where + requires shift
        self.root.bind('<Control-minus>', self.zoom_out)
        self.root.bind('<Command-plus>', self.zoom_in)  # Mac
        self.root.bind('<Command-equal>', self.zoom_in)  # Mac
        self.root.bind('<Command-minus>', self.zoom_out)  # Mac
        
        # Make sure the window can receive focus for key bindings
        self.root.focus_set()
    
    def get_scaled_font_size(self, base_name):
        """Get scaled font size based on current zoom level"""
        return max(8, int(self.base_font_sizes[base_name] * self.font_scale))
    
    def zoom_in(self, event=None):
        """Increase font sizes (zoom in)"""
        if self.font_scale < 2.0:  # Max zoom 200%
            self.font_scale += 0.1
            self.update_fonts()
    
    def zoom_out(self, event=None):
        """Decrease font sizes (zoom out)"""
        if self.font_scale > 0.5:  # Min zoom 50%
            self.font_scale -= 0.1
            self.update_fonts()
    
    def update_fonts(self):
        """Update all fonts with new scale"""
        # Update title
        self.title_label.configure(font=('Arial', self.get_scaled_font_size('title'), 'bold'))
        
        # Update query text
        self.query_text.configure(font=('Consolas', self.get_scaled_font_size('query')))
        
        # Update message and error text areas
        self.messages_text.configure(font=('Consolas', self.get_scaled_font_size('message')))
        self.error_text.configure(font=('Consolas', self.get_scaled_font_size('message')))
        
        # Update status label
        self.status_label.configure(font=('Arial', self.get_scaled_font_size('label')))
        
        # Update table font if it exists
        if self.tree:
            table_font = ('Consolas', self.get_scaled_font_size('table'))
            # Update treeview font
            style = ttk.Style()
            style.configure('Treeview', font=table_font)
            style.configure('Treeview.Heading', font=table_font)
        
    def create_widgets(self):
        # Main container
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Configure grid weights for responsive design
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(1, weight=1)
        main_frame.rowconfigure(3, weight=2)
        
        # Title
        self.title_label = ttk.Label(main_frame, text="SU-SQL Query Interface", 
                               font=('Arial', self.get_scaled_font_size('title'), 'bold'))
        self.title_label.grid(row=0, column=0, sticky=tk.W, pady=(0, 10))
        
        # Query input section
        query_frame = ttk.LabelFrame(main_frame, text="SU-SQL Query", padding="10")
        query_frame.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10))
        query_frame.columnconfigure(0, weight=1)
        query_frame.rowconfigure(0, weight=1)
        
        # Query text area with syntax highlighting colors
        self.query_text = scrolledtext.ScrolledText(
            query_frame, 
            height=8, 
            font=('Consolas', self.get_scaled_font_size('query')),
            bg='#2b2b2b',
            fg='#ffffff',
            insertbackground='white',
            selectbackground='#404040'
        )
        self.query_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Button frame
        button_frame = ttk.Frame(main_frame)
        button_frame.grid(row=2, column=0, sticky=(tk.W, tk.E), pady=(0, 10))
        
        # Execute button
        self.execute_btn = ttk.Button(
            button_frame, 
            text="▶ Execute Query", 
            command=self.execute_query,
            style='Execute.TButton'
        )
        self.execute_btn.pack(side=tk.LEFT, padx=(0, 15))
        
        # Show tables button
        self.show_tables_btn = ttk.Button(
            button_frame, 
            text="📋 Show Tables", 
            command=self.show_tables
        )
        self.show_tables_btn.pack(side=tk.LEFT, padx=(0, 15))
        
        # Clear button
        self.clear_btn = ttk.Button(
            button_frame, 
            text="🗑 Clear Query", 
            command=self.clear_query
        )
        self.clear_btn.pack(side=tk.LEFT, padx=(0, 15))
        
        # Status label
        self.status_var = tk.StringVar()
        self.status_var.set("Ready")
        self.status_label = ttk.Label(button_frame, textvariable=self.status_var, foreground='#00ff88')
        self.status_label.pack(side=tk.RIGHT)
        
        # Results section
        results_frame = ttk.LabelFrame(main_frame, text="Query Results", padding="10")
        results_frame.grid(row=3, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        results_frame.columnconfigure(0, weight=1)
        results_frame.rowconfigure(0, weight=1)
        
        # Create notebook for tabs
        self.notebook = ttk.Notebook(results_frame)
        self.notebook.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Results tab
        self.results_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.results_tab, text="Results")
        
        # Configure results tab
        self.results_tab.columnconfigure(0, weight=1)
        self.results_tab.rowconfigure(0, weight=1)
        
        # Treeview for results
        self.tree_frame = ttk.Frame(self.results_tab)
        self.tree_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        self.tree_frame.columnconfigure(0, weight=1)
        self.tree_frame.rowconfigure(0, weight=1)
        
        # Initially empty - will be created when needed
        self.tree = None
        self.tree_scroll_y = None
        self.tree_scroll_x = None
        
        # Messages tab
        self.messages_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.messages_tab, text="Messages")
        
        self.messages_tab.columnconfigure(0, weight=1)
        self.messages_tab.rowconfigure(0, weight=1)
        
        self.messages_text = scrolledtext.ScrolledText(
            self.messages_tab,
            height=10,
            font=('Consolas', self.get_scaled_font_size('message')),
            bg='#2d2d2d',
            fg='#ffffff',
            insertbackground='white',
            selectbackground='#404040'
        )
        self.messages_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Error tab
        self.error_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.error_tab, text="Errors")
        
        self.error_tab.columnconfigure(0, weight=1)
        self.error_tab.rowconfigure(0, weight=1)
        
        self.error_text = scrolledtext.ScrolledText(
            self.error_tab,
            height=10,
            font=('Consolas', self.get_scaled_font_size('message')),
            bg='#2d2d2d',
            fg='#ff6b6b',
            insertbackground='white',
            selectbackground='#404040'
        )
        self.error_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Bind Enter key to execute query
        self.query_text.bind('<Control-Return>', lambda e: self.execute_query())
        
        # Add some sample queries to help users get started
        sample_query = """-- Sample SU-SQL queries:
-- SELECT * FROM table_name;
-- INSERT INTO table_name (column1, column2) VALUES ('value1', 'value2');
-- SELECT column1, column2 FROM table_name WHERE column1 = 'value';

"""
        self.query_text.insert('1.0', sample_query)
        
    def log_error(self, message):
        """Add error message to the error tab"""
        import datetime
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        formatted_message = f"[{timestamp}] ERROR: {message}\n"
        
        self.error_text.insert(tk.END, formatted_message)
        self.error_text.see(tk.END)
        
        # Switch to error tab to show the error
        self.notebook.select(2)  # Error tab is index 2
        
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
    
    def display_table_results(self, rows):
        """Display query results in a table format"""
        self.clear_results()
        
        if not rows:
            # Show empty result message
            empty_label = ttk.Label(self.tree_frame, text="Query executed successfully. No results to display.",
                                  background='#1e1e1e', foreground='#ffffff')
            empty_label.grid(row=0, column=0, pady=20)
            return
            
        # Get columns from first row
        columns = list(rows[0].keys())
        
        # Create treeview with scrollbars
        self.tree = ttk.Treeview(self.tree_frame, columns=columns, show='headings', height=15)
        
        # Set font for the table
        table_font = ('Consolas', self.get_scaled_font_size('table'))
        style = ttk.Style()
        style.configure('Treeview', font=table_font)
        style.configure('Treeview.Heading', font=table_font)
        
        # Configure column headings and widths
        for col in columns:
            self.tree.heading(col, text=col)
            # Calculate optimal column width
            max_width = max(len(str(col)), max(len(str(row.get(col, ''))) for row in rows))
            self.tree.column(col, width=min(max_width * 8 + 20, 200), minwidth=80)
        
        # Add scrollbars
        self.tree_scroll_y = ttk.Scrollbar(self.tree_frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree_scroll_x = ttk.Scrollbar(self.tree_frame, orient=tk.HORIZONTAL, command=self.tree.xview)
        self.tree.configure(yscrollcommand=self.tree_scroll_y.set, xscrollcommand=self.tree_scroll_x.set)
        
        # Grid layout
        self.tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        self.tree_scroll_y.grid(row=0, column=1, sticky=(tk.N, tk.S))
        self.tree_scroll_x.grid(row=1, column=0, sticky=(tk.W, tk.E))
        
        # Insert data
        for i, row in enumerate(rows):
            values = [str(row.get(col, '')) for col in columns]
            self.tree.insert('', tk.END, values=values)
            
        self.log_message(f"Query executed successfully. {len(rows)} rows returned.")
        
    def display_tables_info(self):
        """Display tables information"""
        self.clear_results()
        
        if not database:
            empty_label = ttk.Label(self.tree_frame, text="No tables found in database.",
                                  background='#1e1e1e', foreground='#ffffff')
            empty_label.grid(row=0, column=0, pady=20)
            return
            
        # Create treeview for tables info
        columns = ['Table Name', 'Total Rows']
        self.tree = ttk.Treeview(self.tree_frame, columns=columns, show='headings', height=15)
        
        # Set font for the table
        table_font = ('Consolas', self.get_scaled_font_size('table'))
        style = ttk.Style()
        style.configure('Treeview', font=table_font)
        style.configure('Treeview.Heading', font=table_font)
        
        # Configure columns
        for col in columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=200, minwidth=100)
        
        # Add scrollbars
        self.tree_scroll_y = ttk.Scrollbar(self.tree_frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=self.tree_scroll_y.set)
        
        # Grid layout
        self.tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        self.tree_scroll_y.grid(row=0, column=1, sticky=(tk.N, tk.S))
        
        # Insert table data
        for table_name, table_rows in database.items():
            self.tree.insert('', tk.END, values=[table_name, len(table_rows)])
            
        self.log_message(f"Found {len(database)} tables in database.")
    
    def execute_query(self):
        """Execute the SU-SQL query"""
        query = self.query_text.get('1.0', tk.END).strip()
        
        if not query or query.startswith('--'):
            self.log_error("Please enter a valid SU-SQL query.")
            return
            
        try:
            self.status_var.set("Executing...")
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
                self.log_error("Empty query.")
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
                self.clear_results()
                success_label = ttk.Label(self.tree_frame, 
                                        text="INSERT executed successfully.", 
                                        foreground='#00ff88',
                                        background='#1e1e1e',
                                        font=('Arial', 12))
                success_label.grid(row=0, column=0, pady=20)
                self.log_message("INSERT statement executed successfully.")
                self.notebook.select(0)  # Switch to results tab
                
            # Add support for UPDATE when you implement it
            elif first_token == "UPDATE":
                self.log_error("UPDATE statement support coming soon!")
                
            else:
                self.log_error(f"Unsupported query type: {first_token}")
                return
                
            self.status_var.set("Query completed")
            
        except KeyError as k:
            error_msg = f"Row/column {k} not found"
            self.log_error(error_msg)
            self.status_var.set("Error")
            
        except Exception as e:
            error_msg = f"Query error: {str(e)}"
            self.log_error(error_msg)
            self.status_var.set("Error")
    
    def show_tables(self):
        """Show all tables in the database"""
        self.display_tables_info()
        self.notebook.select(0)  # Switch to results tab
        self.status_var.set("Tables displayed")
    
    def clear_query(self):
        """Clear the query text area"""
        self.query_text.delete('1.0', tk.END)
        self.status_var.set("Query cleared")

def main():
    root = tk.Tk()
    app = DatabaseGUI(root)
    
    # Center the window
    root.update_idletasks()
    x = (root.winfo_screenwidth() // 2) - (root.winfo_width() // 2)
    y = (root.winfo_screenheight() // 2) - (root.winfo_height() // 2)
    root.geometry(f"+{x}+{y}")
    
    root.mainloop()

if __name__ == "__main__":
    main()
    