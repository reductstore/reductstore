"""Script to reformat long string for Windows"""
import sys

STEP = 16000
if __name__ == "__main__":
    filename = sys.argv[1]
    split = []
    with open(filename, "r") as file:
        while True:
            chunk = file.read(STEP)
            split.append(chunk)
            if len(chunk) < STEP:
                break

    with open(filename, "w") as file:
        file.write('"\n"'.join(split))
