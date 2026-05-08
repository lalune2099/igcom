# -*- coding: utf-8 -*-
"""
Deprecated compatibility wrapper.

Use all.py for the maintained implementation. Runtime credentials are loaded
through config.py instead of being stored in this old duplicate script.
"""

from all import main


if __name__ == "__main__":
    main()
