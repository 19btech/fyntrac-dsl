import os
import sys
import re

download_dir = '/home/uabbas/Downloads/dsl'
clean_dir = '/home/uabbas/Downloads/dsl_clean'

os.makedirs(clean_dir, exist_ok=True)

files_to_clean = [
    'server.py',
    'runtime.py',
    'tools.py',
    'AccountingRuleBuilder.js',
    'ScheduleStepModal.js'
]

for filename in files_to_clean:
    with open(os.path.join(download_dir, filename), 'r') as f:
        lines = f.readlines()
    
    with open(os.path.join(clean_dir, filename), 'w') as f:
        for line in lines:
            # Pattern to match line numbers like "1: "
            # Some lines might be empty or not start with it, but we were told EVERY line starts with it
            m = re.match(r'^\d+:\s?(.*)', line)
            if m:
                # the content could include \n at the end, which the (.*) will capture if we use re.DOTALL, but here we don't.
                # Actually, wait, re.match(r'^\d+:\s?(.*)', line) might drop the newline.
                # Let's just remove the first occurence of "N: "
                # Using a simpler split
                parts = line.split(':', 1)
                if len(parts) == 2 and parts[0].isdigit():
                    content = parts[1]
                    if content.startswith(' '):
                        content = content[1:]
                    f.write(content)
                else:
                    f.write(line)
            else:
                f.write(line)

print("Cleaning done!")
