# Timetable Generator

An automated timetable generation system for academic institutions.

## Features

- Generates timetables for multiple departments and semesters
- Handles lectures, labs, tutorials, and self-study sessions
- Manages faculty schedules and room allocations
- Supports basket courses and electives
- Includes break time management
- Generates detailed reports of unscheduled courses

## Required Files

- `combined.csv`: Main course data
- `rooms.csv`: Room information
- `updated_batches.csv`: Batch size information
- `elective_registration.csv`: Elective course registrations

## Setup

1. Install required dependencies:
```bash
pip install pandas openpyxl
```

2. Prepare input files (CSV format)
3. Run the generator:
```bash
python timetable_gen.py
```

## Output

- Generates `timetable_all.xlsx` with separate sheets for each department/semester
- Includes unscheduled courses report
- Color-coded schedule visualization
