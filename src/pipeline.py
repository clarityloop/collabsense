import argparse
import datetime
import os
import sys
import shutil
import asyncio
from src import config

# import modules
from src import scraper
from src import processor
from src import cleaner

def create_new_run_folder(base_name="run"):
    """Creates a fresh timestamped directory."""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    folder_name = f"{config.OWNER}-{config.REPO}_{base_name}_{timestamp}"
    full_path = os.path.join(config.BASE_DATA_DIR, folder_name)
    os.makedirs(full_path, exist_ok=True)
    return full_path

def main():
    parser = argparse.ArgumentParser(description="ClarityLoop Data Pipeline")
    
    # --- STAGE FLAGS ---
    parser.add_argument('--scrape', action='store_true', help="Run Scraper")
    parser.add_argument('--process', action='store_true', help="Run Processor")
    parser.add_argument('--clean', action='store_true', help="Run Cleaner")
    
    # --- CONFIGURATION ARGS ---
    parser.add_argument('--mode', choices=['standard', 'ltc', 'all'], default='all', help="Processing mode (Standard/LTC)")
    
    # --- INPUT HANDLING ---
    parser.add_argument('--input-file', type=str, help="Path to an existing _FINAL.csv (for Processing step)")
    parser.add_argument('--input-dir', type=str, help="Path to an existing folder (for Cleaning step)")
    
    args = parser.parse_args()

    # if no flags are set, assume full pipeline
    if not (args.scrape or args.process or args.clean):
        args.scrape = args.process = args.clean = True

    # SCRAPER
    if args.scrape:
        print("\n" + "="*40)
        print("STAGE 1: SCRAPING")
        print("="*40)
        
        # create a new folder for this scrape
        run_dir = create_new_run_folder("SCRAPE")
        config.OUTPUT_DIR = run_dir
        print(f"[SETUP] Output Directory: {run_dir}")

        # run Scraper
        asyncio.run(scraper.main())
        
        # automatically pass this output to the next stage if running continuously
        # find the file we just created to pass to the processor
        try:
            import glob
            files = glob.glob(os.path.join(run_dir, "*_FINAL.csv"))
            if files:
                args.input_file = max(files, key=os.path.getctime)
        except Exception:
            print("[WARN] Scraper finished but couldn't auto-detect output file.")

    # PROCESSOR
    if args.process:
        print("\n" + "="*40)
        print("STAGE 2: PROCESSING")
        print("="*40)

        # validation
        if not args.input_file or not os.path.exists(args.input_file):
            print(f"[ERROR] Processing requires --input-file. File not found: {args.input_file}")
            sys.exit(1)

        # create a NEW folder for this processing run
        # (so original scrape folder isnt polluted with multiple experiments)
        run_dir = create_new_run_folder("PROCESS")
        config.OUTPUT_DIR = run_dir
        print(f"[SETUP] Processing Input: {args.input_file}")
        print(f"[SETUP] Output Directory: {run_dir}")

        # load Data
        import pandas as pd
        try:
            raw_data = pd.read_csv(args.input_file)
        except Exception as e:
            print(f"[ERROR] Failed to read CSV: {e}")
            sys.exit(1)

        # run Processor
        if args.mode in ['standard', 'all']:
            processor.run_standard_pipeline(raw_data)
        
        if args.mode in ['ltc', 'all']:
            processor.run_ltc_pipeline(raw_data)

        # pass this directory to the cleaner
        args.input_dir = run_dir

    # CLEANER
    if args.clean:
        print("\n" + "="*40)
        print("STAGE 3: CLEANING")
        print("="*40)

        # validation
        if not args.input_dir or not os.path.exists(args.input_dir):
            print(f"[ERROR] Cleaning requires --input-dir. Directory not found: {args.input_dir}")
            sys.exit(1)

        # point config to the directory containing the processed files
        config.OUTPUT_DIR = args.input_dir
        print(f"[SETUP] Cleaning Directory: {config.OUTPUT_DIR}")

        # run Cleaner
        cleaner.main()

    print("\n" + "="*40)
    print("[PIPELINE] COMPLETE")
    print("="*40)

if __name__ == "__main__":
    main()