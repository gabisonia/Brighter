from pathlib import Path
import hashlib
import json
import struct

base = Path('/capture')
library = Path('/workspace/tests/Paramore.Brighter.Kafka.Tests/bin/Release/net9.0/runtimes/linux-x64/native/centos8-librdkafka.so')
data = library.read_bytes()
assert data[:6] == b'\x7fELF\x02\x01'
assert str(library) in (base / 'pending-close-maps.txt').read_text()
section_offset = struct.unpack_from('<Q', data, 40)[0]
section_size, section_count = struct.unpack_from('<HH', data, 58)
sections = [struct.unpack_from('<IIQQQQIIQQ', data, section_offset + i * section_size) for i in range(section_count)]
symbols = []
for section in sections:
    if section[1] != 2:
        continue
    strings_section = sections[section[6]]
    strings = data[strings_section[4]:strings_section[4] + strings_section[5]]
    for offset in range(section[4], section[4] + section[5], section[9]):
        name, info, _, index, address, length = struct.unpack_from('<IBBHQQ', data, offset)
        if info & 15 == 2 and length and index:
            symbols.append((address, address + length, strings[name:].split(b'\0', 1)[0].decode(errors='replace')))
report = json.loads((base / 'pending-close.core.crashreport.json').read_text(encoding='utf-8-sig'))
threads = []
for thread in report['payload']['threads']:
    native = []
    for frame in thread.get('stack_frames', []):
        if frame.get('native_module') != library.name:
            continue
        address = int(frame['native_image_offset'], 16)
        matches = [{'function': name, 'offset': address - start} for start, end, name in symbols if start <= address < end]
        native.append({'image_offset': frame['native_image_offset'], 'symbols': matches})
    if native:
        managed = [frame.get('method_name') for frame in thread.get('stack_frames', []) if frame.get('method_name')]
        threads.append({'thread_id': thread['native_thread_id'], 'native_frames': native, 'managed_frames': managed})
result = {'library': str(library), 'library_sha256': hashlib.sha256(data).hexdigest(), 'containing_function_symbols': len(symbols),
          'resolved_frames': sum(bool(frame['symbols']) for thread in threads for frame in thread['native_frames']),
          'unresolved_frames': sum(not frame['symbols'] for thread in threads for frame in thread['native_frames']),
          'threads': threads, 'limitation': 'Exact containing function ranges only. No DWARF variables, queue ownership, or complete core claim. Capture followed intrusive healthy-reference pause.'}
(base / 'symbolicated-x64.json').write_text(json.dumps(result, indent=2) + '\n')
close_threads = [thread for thread in threads if any('KafkaMessageConsumer.Close' in method for method in thread['managed_frames'])]
print(json.dumps({'symbol_count': len(symbols), 'resolved_frames': result['resolved_frames'], 'unresolved_frames': result['unresolved_frames'], 'close_threads': close_threads}, indent=2))
