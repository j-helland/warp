# std

# extern

# warp
from warp.workspace import Workspace

# types


__all__ = []


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--session-id', required=True)
    parser.add_argument('--target', required=True)
    args = parser.parse_args()

    ws = Workspace(session_id=args.session_id)
    ws.build(args.target, warp_backfill_call=True)
