import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from cli.shell import main

if __name__ == '__main__':
    main()