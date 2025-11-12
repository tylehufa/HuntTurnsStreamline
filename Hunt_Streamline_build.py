import PyInstaller.__main__
import sys
import shutil
import os
from pathlib import Path

def clean_previous_build():
    """Clean up previous build directories"""
    dirs_to_clean = ['build', 'dist']
    for dir_name in dirs_to_clean:
        if os.path.exists(dir_name):
            print(f"Removing existing {dir_name} directory...")
            shutil.rmtree(dir_name)

def get_playwright_path():
    """Get Playwright browser path"""
    return Path.home() / 'AppData' / 'Local' / 'ms-playwright'

def get_node():
    """Get Node js"""
    return r"C:\Program Files\nodejs"
def get_node_modules_path():
    """ Get Node Libraries"""
    return os.path.join(os.getcwd(),'node_modules')
def get_GVT_search():
    """Get GVT Search.js"""
    return os.path.join(os.getcwd(),'GVT_Search.js')
def get_Hunt_search():
    """Get GVT Search.js"""
    return os.path.join(os.getcwd(),'Hunt_Data_Collection.js')

def create_installer():
    """Create the PyInstaller configuration and run the build"""
    
    clean_previous_build()
    playwright_browser_path = get_playwright_path()
    node_path = get_node()
    node_modules_path = get_node_modules_path()
    GVT_Search = get_GVT_search()
    hunt_path = get_Hunt_search()
       
    installer_args = [
        'JB Hunt streamline.py',
        '--onefile',
        '--name=Hunt_Turns_Streamline7.0',
        
        # GUI and Threading
        '--hidden-import=tkinter',
        '--hidden-import=tkinter.ttk',
        '--hidden-import=threading',
        '--hidden-import=queue',
        
        # Data processing and analysis
        '--hidden-import=pandas',
        '--hidden-import=numpy',
        
        # File and path handling
        '--hidden-import=os',
        '--hidden-import=pathlib',
        '--hidden-import=sys',
        '--hidden-import=shutil',
        
        # Excel handling
        '--hidden-import=openpyxl',
        '--hidden-import=openpyxl.styles',
        '--hidden-import=openpyxl.utils',
        '--hidden-import=openpyxl.worksheet.table',
        '--hidden-import=win32com.client',
        '--hidden-import=pythoncom',
        
        # Date and Time
        '--hidden-import=datetime',
        '--hidden-import=time',
        
        # System utilities
        '--hidden-import=signal',
        '--hidden-import=logging',
        '--hidden-import=subprocess',
        '--hidden-import=concurrent.futures',
        '--hidden-import=json',
        
        # Regular expressions
        '--hidden-import=re',
        
        # Playwright
        '--hidden-import=playwright.sync_api',
        
        # Other utilities
        '--hidden-import=pyperclip',
        '--hidden-import=traceback',
        
        # Collect all required packages
        '--collect-all=pandas',
        '--collect-all=openpyxl',
        '--collect-all=numpy',
        '--collect-all=playwright',

        # Add Playwright browser (you'll need to define playwright_browser_path)
        f'--add-data={playwright_browser_path};ms-playwright',

        f'--add-data={node_path};node',

        f'--add-data={node_modules_path};node_modules',
        f'--add-data={GVT_Search};.',
        f'--add-data={hunt_path};.',
        f'--add-data={os.path.join(os.getcwd(),'package.json')};.',
        # Additional options for better compatibility
        '--copy-metadata=pandas',
        '--copy-metadata=numpy',
        '--copy-metadata=playwright',
        
        # Debug options
        '--debug=imports',
        
        # Runtime hooks
        '--runtime-hook=runttime_hook_playwright.py'
    ]


    try:
        print("Starting PyInstaller build process...")
        PyInstaller.__main__.run(installer_args)
        print("Build completed successfully!")
        
    except Exception as e:
        print(f"Error during build process: {str(e)}")
        sys.exit(1)

def main():
    """Main execution function"""
    try:
        print("Starting build process...")
        create_installer()
        print("Build process completed successfully!")
        
    except Exception as e:
        print(f"Error in main execution: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()    
