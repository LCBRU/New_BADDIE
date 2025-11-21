This repository (New_BADDIE) automates finding, downloading and anonymising DICOM studies.
The codebase is a set of small Python scripts and shell wrappers that expect a mixed Windows/Linux operational environment
and rely heavily on external DICOM tools (DCMTK `findscu`/`movescu`) and the `dicom-anonymizer` CLI.

Use this file to guide an AI coding agent about project intent, structure, and actionable developer workflows.

Summary / Big picture
- Purpose: find DICOM studies from a PACS, download them to a local storage area, anonymise them and track progress.
- Key phases: (1) build a list of StudyInstanceUIDs (`find_dicoms.py`), (2) download studies with `movescu` (`find_dicoms.py`), (3) run anonymisation (`dicom-anonymizer` / `baddie_anon.py`), (4) move/cleanup failed or partial folders (`check_for_failed.py`).
- Why: scripts were built for long-running batch work where restarts are common — the pipeline writes `results_*.csv` and `completed_list_*.csv` so work can resume.

Important files to reference
- `README.md` — user-facing instructions and overall flow (input CSV format, expected outputs).
- `find_dicoms.py` — central orchestration for building `results_{STUDY}.csv`, downloading studies and calling anonymisation. It contains important command-lines for `findscu` and `movescu` and the retry/timeouts logic.
- `baddie_anon.py` — parallel anonymisation of a folder of DICOMs using `dicom-anonymizer` and `pydicom`; shows the pattern for per-file anonymisation and how patient IDs are synthesized.
- `dictionary.json` (root and under study subfolders) — defines which DICOM tags are kept/modified by the anonymiser.
- `check_for_failed.py` — simple post-processing utility that moves folders with too few files into a `failed/` area.
- `convert_to_jpg.py` — example utility to convert DICOM frames to JPEG using `pydicom` + `PIL`.

Project-specific conventions & patterns
- Mixed OS paths: scripts contain both Windows (`V:\`, `C:\Baddie\`) and Linux (`/./media/`, `sudo mkdir`) paths. Confirm the execution environment before changing paths — some scripts are intended to run on a Linux host that mounts a Windows share.
- CLI-first approach: many operations are executed by spawning shell commands (e.g., `dicom-anonymizer`, `findscu`, `movescu`). Prefer minimal edits to the command templates unless you can test the exact runtime environment.
- Restartability: pipeline writes `results_{STUDY}.csv`, `not_found_results_{STUDY}.csv`, and `completed_list_{STUDY}.csv`. Any agent changes must preserve these outputs and their format.
- Anonymiser contract: `dictionary.json` is the single source of truth for tag-mapping. When updating anonymisation, change `dictionary.json` rather than trying to rewrite anonymiser calls.
- Long-running process handling: `find_dicoms.py` contains timeouts, retries, and sleep/backoff logic. Maintain that behavior when refactoring — it encodes operational experience with flaky network and slow `movescu` calls.

External dependencies & integration points
- DCMTK: `findscu`, `movescu` — used for query/retrieve operations against the PACS.
- dicom-anonymizer (PyPI / CLI) — used to anonymise files; scripts call it by shelling out.
- pydicom, dicomanonymizer python package, `alive_progress`, `pandas`, `beautifulsoup4`, `lxml`, `psutil` — used across scripts; confirm `requirements.txt` and runtime Python environment before editing.
- Filesystem mounts and sudo: scripts call `sudo mount` and expect access to network shares. Changes that relax `sudo` use or adjust mounts must be validated with ops.

Actionable guidance for agents working in this repo
- When making changes that alter CLI templates (e.g., `findscu`/`movescu`), include a comment showing the original command and a short test plan describing how to validate the modified command against a test PACS or a small XML sample (`Examle_xml.txt`).
- Preserve the CSV contract: `results_{STUDY}.csv` columns and `completed_list_{STUDY}.csv` behavior — tools downstream and humans rely on these exact files.
- Avoid replacing shell-based anonymisation with library calls unless you add compatibility wrappers that call `dicom-anonymizer` with the same `dictionary.json` behavior.
- When adding new CLI flags, put them in a clearly-named variable near the top of the file and document the reason.
- For refactors: keep the retry/timeouts/backoff logic intact (`TIMEOUT`, `MAX_RETRIES`, `WAIT_AFTER_TIMEOUT`) or port it to an equivalent robust pattern (e.g., use `subprocess.run` with timeouts and consistent logging).

Examples from this codebase
- Download command template (from `find_dicoms.py`):
  - b1 = 'sudo movescu -v -aet XNAT01 -aem XNAT01 +P 104 -aec UHLPACSWFM01 10.194.105.7 104 -S -k QueryRetrieveLevel=STUDY -k StudyInstanceUID='
  - cmd_build = f'{b1}{StudyInstanceUID}{b2}{folder}{b3}'
- Anonymous command pattern (from `baddie_anon.py`):
  - di_cmd = f'dicom-anonymizer --dictionary "{dictionary_loc}dictionary.json" "{in_file}" "{file_out}"'

Notes & gotchas an agent should surface to the user
- Environment-sensitive paths and `sudo` usage — confirm where the script will run (Windows dev box vs Linux host) before changing hard-coded paths.
- Tests: there are no unit tests; for any change that touches download/anonymisation logic, provide a small local test plan (e.g., run on a single `StudyInstanceUID` in a safe test environment or use the included `Examle_xml.txt`).
- Logging: the code appends to `progress.log` and uses `logging` extensively in `find_dicoms.py` — keep log messages informative when changing behavior.

What I couldn't infer (ask the user)
- Which environment should be treated as canonical for running the pipeline (Linux mount host or Windows)?
- Do you want `dicom-anonymizer` invocation migrated to a Python wrapper instead of CLI calls?

If this looks right I will commit this file. Any missing details you want included (for example: preferred Python version, exact mount instructions or CI steps)?
