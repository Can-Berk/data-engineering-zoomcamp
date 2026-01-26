import sys
import pandas as pd


def main():
    # Example: docker run exercise-01
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 5

    df = pd.DataFrame(
        {
            "id": list(range(1, n + 1)),
            "value": [x * 10 for x in range(1, n + 1)],
        }
    )

    out_path = "output.parquet"
    df.to_parquet(out_path, index=False)

    print(f"Wrote {len(df)} rows to {out_path}")


if __name__ == "__main__":
    main()