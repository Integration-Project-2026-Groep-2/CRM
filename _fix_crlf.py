import pathlib

# Use a relative path from the repo root.
p = pathlib.Path("src/sender.py")
c = p.read_bytes()
cr_count = c.count(b"\r")
print(f"CR count: {cr_count}")
if cr_count > 0:
    c = c.replace(b"\r", b"")
    p.write_bytes(c)
    print(f"Cleaned. New CR count: {c.count(b'\r')}")
else:
    print("Already clean.")
