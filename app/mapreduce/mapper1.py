#!/usr/bin/env python3
import sys
import re
import logging

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

def tokenize(text):
    try:
        text_lower = text.lower()
        words = []
        current_word = []
        
        for char in text_lower:
            if char.isalnum() or char == '_':
                current_word.append(char)
            else:
                if current_word:
                    words.append(''.join(current_word))
                    current_word = []
        if current_word:
            words.append(''.join(current_word))
            
        return words
    except Exception as e:
        return []

for line in sys.stdin:
    try:
        parts = line.strip().split("\t", maxsplit=3)
        if len(parts) < 3:
            continue
        
        doc_id, doc_title, doc_text = parts[0], parts[1], parts[2]
        
        for word in tokenize(doc_text):
            print(f"{word}#{doc_id}\t1")
        
    except Exception as e:
        logging.error(f"Critical error: {e}")
        sys.exit(1)