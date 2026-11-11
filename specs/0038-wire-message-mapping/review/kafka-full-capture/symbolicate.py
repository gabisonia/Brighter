from pathlib import Path
import hashlib
import json
import re
import struct
import sys

BASE = Path(__file__).parent
mode = sys.argv[1] if len(sys.argv) > 1 else 'validate'
label = sys.argv[2] if len(sys.argv) > 2 else 'healthy-validation'
assert mode in ('validate', 'full')
out = BASE / mode
library = Path('/private/tmp/brighter-4170-kafka-native-close-diagnostic/bin/Release/net9.0/runtimes/linux-arm64/native/librdkafka.so')
data = library.read_bytes()
assert data[:5] == b'\x7fELF\x02'
endian = '<' if data[5] == 1 else '>'
offset = struct.unpack_from(endian + 'Q', data, 40)[0]
size, count, index = struct.unpack_from(endian + 'HHH', data, 58)
sections = [struct.unpack_from(endian + 'IIQQQQIIQQ', data, offset + i * size) for i in range(count)]
symbols = []
for section in sections:
    if section[1] != 2:  # SHT_SYMTAB, not just the exported dynamic symbols.
        continue
    strings_section = sections[section[6]]
    strings = data[strings_section[4]:strings_section[4] + strings_section[5]]
    for position in range(section[4], section[4] + section[5], section[9]):
        name, info, other, section_index, address, length = struct.unpack_from(endian + 'IBBHQQ', data, position)
        if info & 15 == 2 and length and section_index:
            symbols.append((address, address + length, strings[name:].split(b'\0', 1)[0].decode(errors='replace')))
assert symbols
report = json.loads((out / (label + '.core.crashreport.json')).read_text())
proc = (out / (label + '-proc.txt')).read_text()
assert '/tests/Release/net9.0/runtimes/linux-arm64/native/librdkafka.so' in proc
names = {int(match[1]): match[2].strip() for match in re.finditer(r'TASK /proc/\d+/task/(\d+)\n([^\n]+)', proc)}
threads = []
for thread in report['payload']['threads']:
    frames = []
    for frame in thread.get('stack_frames', []):
        if frame.get('native_module') != 'librdkafka.so':
            continue
        address = int(frame['native_image_offset'], 16)
        matches = [{'name': name, 'offset': address - start} for start, end, name in symbols if start <= address < end]
        frames.append({'image_offset': frame['native_image_offset'], 'symbols': matches})
    if frames:
        threads.append({'thread_id': thread['native_thread_id'], 'thread_name': names.get(int(thread['native_thread_id'], 16)), 'frames': frames})
assert threads
summary = {'library_sha256': hashlib.sha256(data).hexdigest(), 'symbol_table_functions': len(symbols),
           'threads': threads, 'resolved_frames': sum(bool(frame['symbols']) for thread in threads for frame in thread['frames']),
           'unresolved_frames': sum(not frame['symbols'] for thread in threads for frame in thread['frames']),
           'limitation': 'Exact containing ELF function ranges, including aliases; no DWARF type, source-line or variable reconstruction.'}
(out / 'symbolicated-threads.json').write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps({key: value for key, value in summary.items() if key != 'threads'}))
