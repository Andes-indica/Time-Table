import pandas as pd
import random
from datetime import datetime, time, timedelta
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Border, Side, Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter
import csv
import glob
import os

# Academic Settings
SLOT_LENGTH = 2
LECTURE_LENGTH = 3
LAB_LENGTH = 4
TUTORIAL_LENGTH = 2
PREP_LENGTH = 2
GAP_LENGTH = 1

# Schedule Parameters
CLASS_DAYS = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
DAY_BEGIN = time(9, 0)
DAY_END = time(18, 30)

# Lunch break parameters
LUNCH_WINDOW_START = time(12, 30)  # Lunch breaks can start from 12:30
LUNCH_WINDOW_END = time(14, 0)    # Last lunch break must end by 14:00 
LUNCH_DURATION = 60              # Each semester gets 45 min lunch

# Initialize global variables
TIME_SLOTS = []
lunch_breaks = {}  # Global lunch breaks dictionary

def calculate_lunch_breaks(semesters):
    """Dynamically calculate staggered lunch breaks for semesters"""
    global lunch_breaks
    lunch_breaks = {}  # Reset global lunch_breaks
    total_semesters = len(semesters)
    
    if total_semesters == 0:
        return lunch_breaks
        
    # Calculate time between breaks to distribute them evenly
    total_window_minutes = (
        LUNCH_WINDOW_END.hour * 60 + LUNCH_WINDOW_END.minute -
        LUNCH_WINDOW_START.hour * 60 - LUNCH_WINDOW_START.minute
    )
    stagger_interval = (total_window_minutes - LUNCH_DURATION) / (total_semesters - 1) if total_semesters > 1 else 0
    
    # Sort semesters to ensure consistent assignment
    sorted_semesters = sorted(semesters)
    
    for i, semester in enumerate(sorted_semesters):
        start_minutes = (LUNCH_WINDOW_START.hour * 60 + LUNCH_WINDOW_START.minute + 
                        int(i * stagger_interval))
        start_hour = start_minutes // 60
        start_min = start_minutes % 60
        
        end_minutes = start_minutes + LUNCH_DURATION
        end_hour = end_minutes // 60
        end_min = end_minutes % 60
        
        lunch_breaks[semester] = (
            time(start_hour, start_min),
            time(end_hour, end_min)
        )
    
    return lunch_breaks

def initialize_time_slots():
    global TIME_SLOTS
    TIME_SLOTS = generate_time_slots()

def generate_time_slots():
    slots = []
    current_time = datetime.combine(datetime.today(), DAY_BEGIN)
    end_time = datetime.combine(datetime.today(), DAY_END)
    
    while current_time < end_time:
        current = current_time.time()
        next_time = current_time + timedelta(minutes=30)
        
        # Keep all time slots but we'll mark break times later
        slots.append((current, next_time.time()))
        current_time = next_time
    
    return slots

def load_rooms():
    rooms = {}
    try:
        with open('rooms.csv', 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                rooms[row['id']] = {
                    'capacity': int(row['capacity']),
                    'type': row['type'],
                    'roomNumber': row['roomNumber'],
                    'schedule': {day: set() for day in range(len(CLASS_DAYS))}
                }
    except FileNotFoundError:
        print("Warning: rooms.csv not found, using default room allocation")
        return None
    return rooms

def load_batch_data():
    """Load batch information and calculate sections automatically"""
    batch_info = {}
    
    # Load regular batch sizes
    try:
        df = pd.read_csv('updated_batches.csv')
        for _, row in df.iterrows():
            total_students = row['Total_Students']
            max_batch_size = row['MaxBatchSize']
            
            # Calculate number of sections needed
            num_sections = (total_students + max_batch_size - 1) // max_batch_size
            section_size = (total_students + num_sections - 1) // num_sections

            batch_info[(row['Department'], row['Semester'])] = {
                'total': total_students,
                'num_sections': num_sections,
                'section_size': section_size
            }
    except FileNotFoundError:
        print("Warning: updated_batches.csv not found, using default batch sizes")
        
    # Load elective course registrations
    try:
        elective_df = pd.read_csv('elective_registration.csv')
        for _, row in elective_df.iterrows():
            batch_info[('ELECTIVE', row['Course Code'])] = {
                'total': row['Total Students'],
                'num_sections': 1,  # Electives are typically single section
                'section_size': row['Total Students']
            }
    except FileNotFoundError:
        print("Warning: elective_registrations.csv not found")
        
    return batch_info

def find_adjacent_lab_room(room_id, rooms):
    """Find an adjacent lab room based on room numbering"""
    if not room_id:
        return None
    
    # Get room number and extract base info
    current_num = int(''.join(filter(str.isdigit, rooms[room_id]['roomNumber'])))
    current_floor = current_num // 100
    
    # Look for adjacent room with same type
    for rid, room in rooms.items():
        if rid != room_id and room['type'] == rooms[room_id]['type']:
            room_num = int(''.join(filter(str.isdigit, room['roomNumber'])))
            # Check if on same floor and adjacent number
            if room_num // 100 == current_floor and abs(room_num - current_num) == 1:
                return rid
    return None

def find_suitable_room(course_type, department, semester, day, start_slot, duration, rooms, batch_info, timetable, course_code="", used_rooms=None):
    """Find suitable room(s) considering batch sizes and avoiding room conflicts"""
    if not rooms:
        return "DEFAULT_ROOM"
    
    required_capacity = 60  # Default fallback
    is_basket = is_basket_course(course_code)
    
    if batch_info:
        # For elective/basket courses, check elective registrations
        if is_basket:
            elective_info = batch_info.get(('ELECTIVE', course_code))
            if elective_info:
                required_capacity = elective_info['section_size']
        else:
            # For regular courses use department batch info
            dept_info = batch_info.get((department, semester))
            if dept_info:
                required_capacity = dept_info['section_size']

    used_room_ids = set() if used_rooms is None else used_rooms

    # Special handling for labs to get adjacent rooms if needed
    if course_type in ['COMPUTER_LAB', 'HARDWARE_LAB']:
        dept_info = batch_info.get((department, semester))
        if dept_info and dept_info['total'] > 35:  # Standard lab capacity
            # Try to find adjacent lab rooms
            for room_id, room in rooms.items():
                if room_id in used_room_ids or room['type'].upper() != course_type:
                    continue
                    
                # Check if this room is available
                slots_free = True
                for i in range(duration):
                    if start_slot + i in room['schedule'][day]:
                        slots_free = False
                        break
                
                if slots_free:
                    # Try to find an adjacent room
                    adjacent_room = find_adjacent_lab_room(room_id, rooms)
                    if adjacent_room and adjacent_room not in used_room_ids:
                        # Check if adjacent room is also available
                        adjacent_free = True
                        for i in range(duration):
                            if start_slot + i in rooms[adjacent_room]['schedule'][day]:
                                adjacent_free = False
                                break
                        
                        if adjacent_free:
                            # Mark both rooms as used
                            for i in range(duration):
                                room['schedule'][day].add(start_slot + i)
                                rooms[adjacent_room]['schedule'][day].add(start_slot + i)
                            return f"{room_id},{adjacent_room}"  # Return both room IDs
                            
        # If we don't need two rooms or couldn't find adjacent ones, use regular allocation
        return try_room_allocation(rooms, course_type, required_capacity, day, start_slot, duration, used_room_ids)

    # For lectures and basket courses, try different room types in priority order
    if course_type in ['LEC', 'TUT', 'SS'] or is_basket:
        # First try regular lecture rooms
        lecture_rooms = {rid: room for rid, room in rooms.items() 
                        if 'LECTURE_ROOM' in room['type'].upper()}
        
        # Then try large seater rooms 
        seater_rooms = {rid: room for rid, room in rooms.items()
                       if 'SEATER' in room['type'].upper()}
        
        # For basket courses, need special room allocation
        if is_basket:
            basket_group = get_basket_group(course_code)
            basket_used_rooms = set()
            basket_group_rooms = {}  # Track rooms already allocated to this basket group
            
            # Track room usage count
            room_usage = {rid: sum(len(room['schedule'][d]) for d in range(len(CLASS_DAYS))) 
                         for rid, room in rooms.items()}
            
            # Sort lecture rooms by usage count
            sorted_lecture_rooms = dict(sorted(lecture_rooms.items(), 
                                             key=lambda x: room_usage[x[0]]))
            sorted_seater_rooms = dict(sorted(seater_rooms.items(),
                                            key=lambda x: room_usage[x[0]]))
            
            # Check room availability for the sorted rooms
            for room_dict in [sorted_lecture_rooms, sorted_seater_rooms]:
                for room_id, room in room_dict.items():
                    is_used = False
                    for slot in range(start_slot, start_slot + duration):
                        if slot in rooms[room_id]['schedule'][day]:
                            # Check if room is used by any course from same basket group
                            if slot in timetable[day]:
                                slot_data = timetable[day][slot]
                                if (slot_data['classroom'] == room_id and 
                                    slot_data['type'] is not None):
                                    slot_code = slot_data.get('code', '')
                                    if get_basket_group(slot_code) == basket_group:
                                        basket_group_rooms[slot_code] = room_id
                                    else:
                                        basket_used_rooms.add(room_id)
                            is_used = True
                            break
                    
                    # Room is free for this time slot
                    if not is_used and room_id not in basket_used_rooms:
                        if 'capacity' in room and room['capacity'] >= required_capacity:
                            # Mark slots as used
                            for i in range(duration):
                                room['schedule'][day].add(start_slot + i)
                            return room_id
            
            # If no unused room found, try existing basket group rooms
            if course_code in basket_group_rooms:
                return basket_group_rooms[course_code]
            
            # Try remaining rooms through regular allocation
            room_id = try_room_allocation(lecture_rooms, 'LEC', required_capacity,
                                        day, start_slot, duration, basket_used_rooms)
            
            if not room_id:
                room_id = try_room_allocation(seater_rooms, 'LEC', required_capacity,
                                            day, start_slot, duration, basket_used_rooms)
            
            if room_id:
                basket_group_rooms[course_code] = room_id
            
            return room_id

        # For non-basket courses, use original logic
        room_id = try_room_allocation(lecture_rooms, 'LEC', required_capacity,
                                    day, start_slot, duration, used_room_ids)
        if not room_id:
            room_id = try_room_allocation(seater_rooms, 'LEC', required_capacity,
                                        day, start_slot, duration, used_room_ids)
        return room_id
    
    # For labs, use existing logic
    return try_room_allocation(rooms, course_type, required_capacity,
                             day, start_slot, duration, used_room_ids)

def try_room_allocation(rooms, course_type, required_capacity, day, start_slot, duration, used_room_ids):
    """Helper function to try allocating rooms of a certain type"""
    for room_id, room in rooms.items():
        if room_id in used_room_ids or room['type'].upper() == 'LIBRARY':
            continue
            
        # For lectures and tutorials, only use lecture rooms and seater rooms
        if course_type in ['LEC', 'TUT', 'SS']:
            if not ('LECTURE_ROOM' in room['type'].upper() or 'SEATER' in room['type'].upper()):
                continue
        # For labs, match lab type exactly
        elif course_type == 'COMPUTER_LAB' and room['type'].upper() != 'COMPUTER_LAB':
            continue
        elif course_type == 'HARDWARE_LAB' and room['type'].upper() != 'HARDWARE_LAB':
            continue
            
        # Check capacity except for labs which can be split into batches
        if course_type not in ['COMPUTER_LAB', 'HARDWARE_LAB'] and room['capacity'] < required_capacity:
            continue

        # Check availability
        slots_free = True
        for i in range(duration):
            if start_slot + i in room['schedule'][day]:
                slots_free = False
                break
                
        if slots_free:
            for i in range(duration):
                room['schedule'][day].add(start_slot + i)
            return room_id
                
    return None

def get_required_room_type(course):
    """Determine required room type based on course attributes"""
    if pd.notna(course['P']) and course['P'] > 0:
        course_code = str(course['Course Code']).upper()
        # For CS courses, use computer labs
        if 'CS' in course_code or 'DS' in course_code:
            return 'COMPUTER_LAB'
        # For EC courses, use hardware labs
        elif 'EC' in course_code:
            return 'HARDWARE_LAB'
        return 'COMPUTER_LAB'  # Default to computer lab if unspecified
    else:
        # For lectures, tutorials, and self-study
        return 'LECTURE_ROOM'

# Add this function to help identify basket courses
def is_basket_course(code):
    """Check if course is part of a basket based on code prefix"""
    return code.startswith('B') and '-' in code

def get_basket_group(code):
    """Get the basket group (B1, B2 etc) from course code"""
    if is_basket_course(code):
        return code.split('-')[0]
    return None

def get_basket_group_slots(timetable, day, basket_group):
    """Find existing slots with courses from same basket group"""
    basket_slots = []
    for slot_idx, slot in timetable[day].items():
        code = slot.get('code', '')
        if code and get_basket_group(code) == basket_group:
            basket_slots.append(slot_idx)
    return basket_slots

# Load data from CSV with robust error handling
try:
    # Try different encodings and handle BOM
    encodings_to_try = ['utf-8-sig', 'utf-8', 'cp1252']
    df = None
    last_error = None
    
    for encoding in encodings_to_try:
        try:
            df = pd.read_csv('combined.csv', encoding=encoding)
            # Convert empty strings and 'nan' strings to actual NaN
            df = df.replace(r'^\s*$', pd.NA, regex=True)
            df = df.replace('nan', pd.NA)
            break
        except UnicodeDecodeError:
            continue
        except Exception as e:
            last_error = e
            continue
            
    if df is None:
        print(f"Error: Unable to read combined.csv. Please check the file format.\nDetails: {str(last_error)}")
        exit()
        
except Exception as e:
    print(f"Error: Failed to load combined.csv.\nDetails: {str(e)}")
    exit()

if df.empty:
    print("Error: No data found in combined.csv")
    exit()

def is_break_time(slot, semester=None):
    """Check if a time slot falls within break times"""
    global lunch_breaks
    start, end = slot
    
    # Morning break: 10:30-11:00
    morning_break = (time(10, 30) <= start < time(11, 0))
    
    # Staggered lunch breaks based on semester
    lunch_break = False
    if semester:
        base_sem = int(str(semester)[0])  # Get base semester number (e.g., 4 from 4A)
        if base_sem in lunch_breaks:
            lunch_start, lunch_end = lunch_breaks[base_sem]
            lunch_break = (lunch_start <= start < lunch_end)
    else:
        # For general checks without semester info, block all lunch periods
        lunch_break = any(lunch_start <= start < lunch_end 
                         for lunch_start, lunch_end in lunch_breaks.values())
    
    return morning_break or lunch_break

def is_lecture_scheduled(timetable, day, start_slot, end_slot):
    """Check if there's a lecture scheduled in the given time range"""
    for slot in range(start_slot, end_slot):
        if (slot < len(timetable[day]) and 
            timetable[day][slot]['type'] and 
            timetable[day][slot]['type'] in ['LEC', 'LAB', 'TUT']):
            return True
    return False

def calculate_required_slots(course):
    """Calculate how many slots needed based on L, T, P, S values and credits"""
    l = float(course['L']) if pd.notna(course['L']) else 0  # Lecture credits
    t = int(course['T']) if pd.notna(course['T']) else 0    # Tutorial hours
    p = int(course['P']) if pd.notna(course['P']) else 0    # Lab hours
    s = int(course['S']) if pd.notna(course['S']) else 0    # Self study hours
    c = int(course['C']) if pd.notna(course['C']) else 0    # Total credits
    
    # Check if course is self-study only
    if s > 0 and l == 0 and t == 0 and p == 0:
        return 0, 0, 0, 0
        
    # Calculate number of lecture sessions based on credits
    lecture_sessions = 0
    if l > 0:
        # For 3 credits = 2 sessions of 1.5 hours each
        # For 2 credits = 1 session of 1.5 hours plus a 1 hour session
        # For 1 credit = 1 session of 1.5 hours
        lecture_sessions = max(1, round(l * 2/3))  # Scale credits to sessions
    
    # Other calculations remain the same
    tutorial_sessions = t  
    lab_sessions = p // 2  # 2 hours per lab session
    self_study_sessions = s // 4 if (l > 0 or t > 0 or p > 0) else 0
    
    return lecture_sessions, tutorial_sessions, lab_sessions, self_study_sessions

def select_faculty(faculty_str):
    """Select a faculty from potentially multiple options."""
    if '/' in faculty_str:
        # Split by slash and strip whitespace
        faculty_options = [f.strip() for f in faculty_str.split('/')]
        return faculty_options[0]  # Take first faculty as default
    return faculty_str

def check_faculty_daily_components(professor_schedule, faculty, day, department, semester, section, timetable, course_code=None, activity_type=None):
    """Check faculty/course scheduling constraints for the day"""
    component_count = 0
    faculty_courses = set()  # Track faculty's courses 
    
    # Check all slots for this day
    for slot in timetable[day].values():
        if slot['faculty'] == faculty and slot['type'] in ['LEC', 'LAB', 'TUT']:
            slot_code = slot.get('code', '')
            if slot_code:
                # For non-basket courses
                if not is_basket_course(slot_code):
                    component_count += 1
                # For basket courses, only count if not already counted
                elif slot_code not in faculty_courses:
                    component_count += 1
                    faculty_courses.add(slot_code)
                    
    # Special handling for basket courses - allow parallel scheduling
    if course_code and is_basket_course(course_code):
        basket_group = get_basket_group(course_code)
        existing_slots = get_basket_group_slots(timetable, day, basket_group)
        if existing_slots:
            # For basket courses, check only non-basket components
            return component_count < 3  # Allow more flexibility for basket courses
    
    return component_count < 2  # Keep max 2 components per day limit for regular courses

def check_faculty_course_gap(professor_schedule, timetable, faculty, course_code, day, start_slot):
    """Check if there is sufficient gap (3 hours) between sessions of same course"""
    min_gap_hours = 3
    slots_per_hour = 2  # Assuming 30-min slots
    required_gap = min_gap_hours * slots_per_hour
    
    # Check previous slots
    for i in range(max(0, start_slot - required_gap), start_slot):
        if i in professor_schedule[faculty][day]:
            slot_data = timetable[day][i]
            if slot_data['code'] == course_code and slot_data['type'] in ['LEC', 'TUT']:
                return False
                
    # Check next slots  
    for i in range(start_slot + 1, min(len(TIME_SLOTS), start_slot + required_gap)):
        if i in professor_schedule[faculty][day]:
            slot_data = timetable[day][i]
            if slot_data['code'] == course_code and slot_data['type'] in ['LEC', 'TUT']:
                return False
    
    return True

def load_reserved_slots():
    """Return empty reserved slots structure"""
    return {day: {} for day in CLASS_DAYS}

def is_slot_reserved(slot, day, semester, department, reserved_slots):
    """Check if a time slot is reserved"""
    # Since we're not using reserved_slots.csv anymore, 
    # this function always returns False
    return False

def get_course_priority(course):
    """Calculate course scheduling priority based on constraints"""
    priority = 0
    code = str(course['Course Code'])
    
    # Give regular course labs highest priority with much higher weight
    if pd.notna(course['P']) and course['P'] > 0 and not is_basket_course(code):
        priority += 10  # Increased from 5 to 10 for regular labs
        if 'CS' in code or 'EC' in code:  # Extra priority for CS/EC labs
            priority += 2
    elif is_basket_course(code):
        priority += 1  # Keep lowest priority for basket courses
    elif pd.notna(course['L']) and course['L'] > 2:
        priority += 3  # Regular lectures priority
    elif pd.notna(course['T']) and course['T'] > 0:
        priority += 2  # Tutorial priority
    return priority

def get_best_slots(timetable, professor_schedule, faculty, day, duration, reserved_slots, semester, department):
    """Find best available consecutive slots in a day"""
    available_slots = []
    
    for start_slot in range(len(TIME_SLOTS) - duration + 1):
        slots_free = True
        # Check each slot in the duration
        for i in range(duration):
            current_slot = start_slot + i
            if (current_slot in professor_schedule[faculty][day] or
                timetable[day][current_slot]['type'] is not None or
                is_break_time(TIME_SLOTS[current_slot], semester) or
                is_slot_reserved(TIME_SLOTS[current_slot], CLASS_DAYS[day], semester, department, reserved_slots)):
                slots_free = False
                break

        if slots_free:
            available_slots.append(start_slot)
    
    return available_slots

def print_unscheduled_courses(unscheduled_courses):
    """Print a summary of unscheduled courses to the console"""
    if not unscheduled_courses:
        print("All courses were successfully scheduled!")
        return
        
    print("\n" + "="*80)
    print("UNSCHEDULED COURSES SUMMARY".center(80))
    print("="*80)
    print(f"{'Department':<10} {'Semester':<10} {'Course Code':<15} {'Course Name':<30} {'Faculty':<20} {'Missing':<8}")
    print("-"*80)
    
    for course in unscheduled_courses:
        missing = course['Expected Slots'] - course['Scheduled Slots']
        print(f"{course['Department']:<10} {course['Semester']:<10} {course['Code']:<15} {course['Name'][:28]:<30} {course['Faculty'][:18]:<20} {missing:<8}")
    
    print("="*80)
    print(f"Total unscheduled courses: {len(unscheduled_courses)}")
    print("="*80)

def generate_all_timetables():
    global lunch_breaks
    initialize_time_slots()
    reserved_slots = load_reserved_slots()
    wb = Workbook()
    wb.remove(wb.active)
    professor_schedule = {}
    rooms = load_rooms()
    batch_info = load_batch_data()

    # Track unscheduled courses
    unscheduled_courses = []

    # Get all unique semester numbers
    all_semesters = sorted(set(int(str(sem)[0]) for sem in df['Semester'].unique()))
    # Calculate lunch breaks dynamically
    lunch_breaks = calculate_lunch_breaks(all_semesters)

    for department in df['Department'].unique():
        # Track assigned faculty for courses
        course_faculty_assignments = {}
        
        # Process all semesters for this department
        for semester in df[df['Department'] == department]['Semester'].unique():
            # Filter out courses marked as not to be scheduled
            courses = df[(df['Department'] == department) & 
                        (df['Semester'] == semester) & 
                        ((df['Schedule'].fillna('Yes').str.upper() == 'YES') | 
                         (df['Schedule'].isna()))].copy()
            
            if courses.empty:
                continue

            # First handle lab scheduling as a separate pass
            lab_courses = courses[courses['P'] > 0].copy()
            lab_courses['priority'] = lab_courses.apply(get_course_priority, axis=1)
            lab_courses = lab_courses.sort_values('priority', ascending=False)

            # Handle remaining courses after labs
            non_lab_courses = courses[courses['P'] == 0].copy()
            non_lab_courses['priority'] = non_lab_courses.apply(get_course_priority, axis=1)
            non_lab_courses = non_lab_courses.sort_values('priority', ascending=False)

            # Combine sorted courses with labs first
            courses = pd.concat([lab_courses, non_lab_courses])

            # Get section info
            dept_info = batch_info.get((department, semester))
            num_sections = dept_info['num_sections'] if dept_info else 1

            for section in range(num_sections):
                section_title = f"{department}{semester}" if num_sections == 1 else f"{department}{semester}_{chr(65+section)}"
                ws = wb.create_sheet(title=section_title)
                
                # Initialize timetable structure
                timetable = {day: {slot: {'type': None, 'code': '', 'name': '', 'faculty': '', 'classroom': ''} 
                         for slot in range(len(TIME_SLOTS))} for day in range(len(CLASS_DAYS))}
                
                # Sort courses by priority
                courses['priority'] = courses.apply(get_course_priority, axis=1)
                courses = courses.sort_values('priority', ascending=False)

                # Process all courses - both lab and non-lab
                for _, course in courses.iterrows():
                    code = str(course['Course Code'])
                    name = str(course['Course Name'])
                    faculty = str(course['Faculty'])
                    
                    # Track if course was scheduled
                    course_scheduled = False
                    lecture_sessions, tutorial_sessions, lab_sessions, _ = calculate_required_slots(course)
                    
                    if faculty not in professor_schedule:
                        professor_schedule[faculty] = {day: set() for day in range(len(CLASS_DAYS))}

                    # Schedule lectures with tracking
                    for _ in range(lecture_sessions):
                        scheduled = False
                        attempts = 0
                        while not scheduled and attempts < 1000:
                            day = random.randint(0, len(CLASS_DAYS)-1)
                            start_slot = random.randint(0, len(TIME_SLOTS)-LECTURE_LENGTH)
                            
                            # Add check for faculty-course gap
                            if not check_faculty_course_gap(professor_schedule, timetable, faculty, code, day, start_slot):
                                attempts += 1
                                continue
                            
                            # Check if any slot in the range is reserved
                            slots_reserved = any(is_slot_reserved(TIME_SLOTS[start_slot + i], 
                                                                CLASS_DAYS[day],
                                                                semester,
                                                                department,
                                                                reserved_slots) 
                                               for i in range(LECTURE_LENGTH))
                            
                            if slots_reserved:
                                attempts += 1
                                continue
                            
                            # Check faculty daily component limit and lecture constraints
                            if not check_faculty_daily_components(professor_schedule, faculty, day, 
                                                               department, semester, section, timetable,
                                                               code, 'LEC'):
                                attempts += 1
                                continue
                                
                            # Check availability and ensure breaks between lectures
                            slots_free = True
                            for i in range(LECTURE_LENGTH):
                                current_slot = start_slot + i
                                if (current_slot in professor_schedule[faculty][day] or 
                                    timetable[day][current_slot]['type'] is not None or
                                    is_break_time(TIME_SLOTS[current_slot], semester)):
                                    slots_free = False
                                    break
                                
                                # Check for lectures before this slot
                                if current_slot > 0:
                                    if is_lecture_scheduled(timetable, day, 
                                                         max(0, current_slot - GAP_LENGTH), 
                                                         current_slot):
                                        slots_free = False
                                        break
                                
                                # Check for lectures after this slot
                                if current_slot < len(TIME_SLOTS) - 1:
                                    if is_lecture_scheduled(timetable, day,
                                                         current_slot + 1,
                                                         min(len(TIME_SLOTS), 
                                                             current_slot + GAP_LENGTH + 1)):
                                        slots_free = False
                                        break
                            
                            if slots_free:
                                room_id = find_suitable_room('LECTURE_ROOM', department, semester, 
                                                          day, start_slot, LECTURE_LENGTH, 
                                                          rooms, batch_info, timetable, code)
                                
                                if room_id:
                                    classroom = room_id
                                    
                                    # Mark slots as used
                                    for i in range(LECTURE_LENGTH):
                                        professor_schedule[faculty][day].add(start_slot+i)
                                        timetable[day][start_slot+i]['type'] = 'LEC'
                                        timetable[day][start_slot+i]['code'] = code if i == 0 else ''
                                        timetable[day][start_slot+i]['name'] = name if i == 0 else ''
                                        timetable[day][start_slot+i]['faculty'] = faculty if i == 0 else ''
                                        timetable[day][start_slot+i]['classroom'] = classroom if i == 0 else ''
                                    scheduled = True
                            attempts += 1

                    # Schedule tutorials with tracking
                    for _ in range(tutorial_sessions):
                        scheduled = False
                        attempts = 0
                        while not scheduled and attempts < 1000:
                            day = random.randint(0, len(CLASS_DAYS)-1)
                            
                            # Add check for faculty-course gap
                            if not check_faculty_course_gap(professor_schedule, timetable, faculty, code, day, start_slot):
                                attempts += 1
                                continue
                            
                            # Check faculty daily component limit for tutorials
                            if not check_faculty_daily_components(professor_schedule, faculty, day,
                                                               department, semester, section, timetable,
                                                               code, 'TUT'):
                                attempts += 1
                                continue
                                
                            start_slot = random.randint(0, len(TIME_SLOTS)-TUTORIAL_LENGTH)
                            
                            # Check if any slot in the range is reserved
                            slots_reserved = any(is_slot_reserved(TIME_SLOTS[start_slot + i], 
                                                                CLASS_DAYS[day],
                                                                semester,
                                                                department,
                                                                reserved_slots) 
                                               for i in range(TUTORIAL_LENGTH))
                            
                            if slots_reserved:
                                attempts += 1
                                continue
                            
                            # Check availability
                            slots_free = True
                            for i in range(TUTORIAL_LENGTH):
                                if (start_slot+i in professor_schedule[faculty][day] or 
                                    timetable[day][start_slot+i]['type'] is not None or
                                    is_break_time(TIME_SLOTS[start_slot+i], semester)):
                                    slots_free = False
                                    break
                            
                            if slots_free:
                                room_id = find_suitable_room('LECTURE_ROOM', department, semester, 
                                                          day, start_slot, TUTORIAL_LENGTH, 
                                                          rooms, batch_info, timetable, code)
                                
                                if room_id:
                                    classroom = room_id
                                    
                                    # Mark slots as used
                                    for i in range(TUTORIAL_LENGTH):
                                        professor_schedule[faculty][day].add(start_slot+i)
                                        timetable[day][start_slot+i]['type'] = 'TUT'
                                        timetable[day][start_slot+i]['code'] = code if i == 0 else ''
                                        timetable[day][start_slot+i]['name'] = name if i == 0 else ''
                                        timetable[day][start_slot+i]['faculty'] = faculty if i == 0 else ''
                                        timetable[day][start_slot+i]['classroom'] = classroom if i == 0 else ''
                                    scheduled = True
                            attempts += 1

                    # Schedule labs with tracking
                    if lab_sessions > 0:
                        room_type = get_required_room_type(course)
                        for _ in range(lab_sessions):
                            scheduled = False
                            attempts = 0
                            
                            # Try each day in random order
                            days = list(range(len(CLASS_DAYS)))
                            random.shuffle(days)
                            
                            for day in days:
                                # Get all possible slots for this day
                                possible_slots = get_best_slots(timetable, professor_schedule, 
                                                              faculty, day, LAB_LENGTH, 
                                                              reserved_slots, semester, department)
                                
                                for start_slot in possible_slots:
                                    room_id = find_suitable_room(room_type, department, semester,
                                                               day, start_slot, LAB_LENGTH,
                                                               rooms, batch_info, timetable, code)
                                    
                                    if room_id:
                                        classroom = room_id if ',' not in str(room_id) else f"{room_id.split(',')[0]}+{room_id.split(',')[1]}"
                                        
                                        # Mark slots as used
                                        for i in range(LAB_LENGTH):
                                            professor_schedule[faculty][day].add(start_slot+i)
                                            timetable[day][start_slot+i]['type'] = 'LAB'
                                            timetable[day][start_slot+i]['code'] = code if i == 0 else ''
                                            timetable[day][start_slot+i]['name'] = name if i == 0 else ''
                                            timetable[day][start_slot+i]['faculty'] = faculty if i == 0 else ''
                                            timetable[day][start_slot+i]['classroom'] = classroom if i == 0 else ''
                                        scheduled = True
                                        break
                                
                                if scheduled:
                                    break

                # Schedule self-study sessions
                for _, course in courses.iterrows():
                    code = str(course['Course Code'])
                    name = str(course['Course Name'])
                    faculty = str(course['Faculty'])
                    _, _, _, self_study_sessions = calculate_required_slots(course)
                    
                    if self_study_sessions > 0:
                        if faculty not in professor_schedule:
                            professor_schedule[faculty] = {day: set() for day in range(len(CLASS_DAYS))}
                        
                        # Schedule each self-study session (1 hour each)
                        for _ in range(self_study_sessions):
                            scheduled = False
                            attempts = 0
                            while not scheduled and attempts < 1000:
                                day = random.randint(0, len(CLASS_DAYS)-1)
                                start_slot = random.randint(0, len(TIME_SLOTS)-PREP_LENGTH)
                                
                                # Check if any slot in the range is reserved
                                slots_reserved = any(is_slot_reserved(TIME_SLOTS[start_slot + i], 
                                                                    CLASS_DAYS[day],
                                                                    semester,
                                                                    department,
                                                                    reserved_slots) 
                                                   for i in range(PREP_LENGTH))
                                
                                if slots_reserved:
                                    attempts += 1
                                    continue
                                
                                # Check availability
                                slots_free = True
                                for i in range(PREP_LENGTH):
                                    if (start_slot+i in professor_schedule[faculty][day] or 
                                        timetable[day][start_slot+i]['type'] is not None or
                                        is_break_time(TIME_SLOTS[start_slot+i], semester)):
                                        slots_free = False
                                        break
                                
                                if slots_free:
                                    room_id = find_suitable_room('LECTURE_ROOM', department, semester, 
                                                              day, start_slot, PREP_LENGTH, 
                                                              rooms, batch_info, timetable, code)
                                    
                                    if room_id:
                                        classroom = room_id
                                        
                                        # Mark slots as used
                                        for i in range(PREP_LENGTH):
                                            professor_schedule[faculty][day].add(start_slot+i)
                                            timetable[day][start_slot+i]['type'] = 'SS'  # SS for Self Study
                                            timetable[day][start_slot+i]['code'] = code if i == 0 else ''
                                            timetable[day][start_slot+i]['name'] = name if i == 0 else ''
                                            timetable[day][start_slot+i]['faculty'] = faculty if i == 0 else ''
                                            timetable[day][start_slot+i]['classroom'] = classroom if i == 0 else ''
                                        scheduled = True
                                attempts += 1

                # Write timetable to worksheet
                period_times = ['Day'] + [f"{t[0].strftime('%H:%M')}-{t[1].strftime('%H:%M')}" for t in TIME_SLOTS]
                ws.append(period_times)
                
                # Style header formatting
                head_style = Font(bold=True)
                cell_center = Alignment(horizontal='center', vertical='center')
                
                for header_cell in ws[1]:
                    header_cell.font = head_style
                    header_cell.alignment = cell_center
                
                # Activity color scheme
                lecture_style = PatternFill(start_color="87CEEB", end_color="87CEEB", fill_type="solid")  # Different blue
                practical_style = PatternFill(start_color="FAE5D3", end_color="FAE5D3", fill_type="solid")  # Different skin
                tutorial_style = PatternFill(start_color="FFB347", end_color="FFB347", fill_type="solid")  # Different orange
                
                grid_border = Border(left=Side(style='thin'), right=Side(style='thin'),
                                   top=Side(style='thin'), bottom=Side(style='thin'))
                
                # Write timetable grid
                for day_idx, day in enumerate(CLASS_DAYS):
                    row_num = day_idx + 2
                    ws.append([day])
                    
                    merge_ranges = []  # Track merge ranges for this row
                    
                    for slot_idx in range(len(TIME_SLOTS)):
                        cell_value = ''
                        cell_fill = None
                        
                        if is_break_time(TIME_SLOTS[slot_idx], semester):
                            cell_value = "BREAK"
                        elif timetable[day_idx][slot_idx]['type']:
                            activity_type = timetable[day_idx][slot_idx]['type']
                            code = timetable[day_idx][slot_idx]['code']
                            classroom = timetable[day_idx][slot_idx]['classroom']
                            faculty = timetable[day_idx][slot_idx]['faculty']
                            
                            if code:
                                duration = {
                                    'LEC': LECTURE_LENGTH,
                                    'LAB': LAB_LENGTH,
                                    'TUT': TUTORIAL_LENGTH,
                                    'SS': PREP_LENGTH
                                }.get(activity_type, 1)
                                
                                # Apply colors based on activity type
                                cell_fill = {
                                    'LEC': lecture_style,
                                    'LAB': practical_style,
                                    'TUT': tutorial_style
                                }.get(activity_type)
                                
                                cell_value = f"{code} {activity_type}\n{classroom}\n{faculty}"
                                
                                # Create merge range
                                if duration > 1:
                                    start_col = get_column_letter(slot_idx + 2)
                                    end_col = get_column_letter(slot_idx + duration + 1)
                                    merge_range = f"{start_col}{row_num}:{end_col}{row_num}"
                                    merge_ranges.append((merge_range, cell_fill))
                        
                        cell = ws.cell(row=row_num, column=slot_idx+2, value=cell_value)
                        if cell_fill:
                            cell.fill = cell_fill
                        cell.border = grid_border
                        cell.alignment = Alignment(wrap_text=True, vertical='center', horizontal='center')
                    
                    # Apply merges after creating all cells in the row
                    for merge_range, fill in merge_ranges:
                        ws.merge_cells(merge_range)
                        merged_cell = ws[merge_range.split(':')[0]]
                        if fill:
                            merged_cell.fill = fill
                        merged_cell.alignment = Alignment(wrap_text=True, vertical='center', horizontal='center')

                # Set column widths and row heights
                for col_idx in range(1, len(TIME_SLOTS)+2):
                    col_letter = get_column_letter(col_idx)
                    ws.column_dimensions[col_letter].width = 15
                
                for row in ws.iter_rows(min_row=2, max_row=len(CLASS_DAYS)+1):
                    ws.row_dimensions[row[0].row].height = 40

                # Add unscheduled courses section
                dept_courses = df[(df['Department'] == department) & 
                                 (df['Semester'] == semester) &
                                 ((df['Schedule'].fillna('Yes').str.upper() == 'YES') | 
                                  (df['Schedule'].isna()))].copy()

                # Add empty rows for spacing
                ws.append([])
                ws.append([])

                # Add unscheduled courses header
                header_row = ws.max_row + 1
                ws.cell(row=header_row, column=1, value="Unscheduled Courses").font = Font(bold=True)
                ws.merge_cells(start_row=header_row, start_column=1, end_row=header_row, end_column=6)

                # Add column headers
                headers = ['Course Code', 'Course Name', 'Faculty', 'Required Components', 'Missing Components']
                ws.append(headers)
                for idx, header in enumerate(headers, 1):
                    cell = ws.cell(row=header_row + 1, column=idx)
                    cell.font = Font(bold=True)
                    cell.alignment = Alignment(horizontal='center')

                # Check each course for unscheduled components
                for _, course in dept_courses.iterrows():
                    code = str(course['Course Code'])
                    name = str(course['Course Name'])
                    faculty = str(course['Faculty'])
                    
                    # Calculate required components
                    lecture_sessions, tutorial_sessions, lab_sessions, self_study = calculate_required_slots(course)
                    required = []
                    if lecture_sessions > 0: required.append(f"LEC:{lecture_sessions}")
                    if tutorial_sessions > 0: required.append(f"TUT:{tutorial_sessions}")
                    if lab_sessions > 0: required.append(f"LAB:{lab_sessions}")
                    if self_study > 0: required.append(f"SS:{self_study}")
                    
                    # Count scheduled components
                    scheduled_lec = sum(1 for day in range(len(CLASS_DAYS)) 
                                       for slot in range(len(TIME_SLOTS)) 
                                       if timetable[day][slot]['code'] == code 
                                       and timetable[day][slot]['type'] == 'LEC')
                    
                    scheduled_tut = sum(1 for day in range(len(CLASS_DAYS))
                                       for slot in range(len(TIME_SLOTS))
                                       if timetable[day][slot]['code'] == code
                                       and timetable[day][slot]['type'] == 'TUT')
                    
                    scheduled_lab = sum(1 for day in range(len(CLASS_DAYS))
                                       for slot in range(len(TIME_SLOTS))
                                       if timetable[day][slot]['code'] == code
                                       and timetable[day][slot]['type'] == 'LAB')
                    
                    scheduled_ss = sum(1 for day in range(len(CLASS_DAYS))
                                      for slot in range(len(TIME_SLOTS))
                                      if timetable[day][slot]['code'] == code
                                      and timetable[day][slot]['type'] == 'SS')
                    
                    # Calculate missing components
                    missing = []
                    if scheduled_lec < lecture_sessions: missing.append(f"LEC:{lecture_sessions-scheduled_lec}")
                    if scheduled_tut < tutorial_sessions: missing.append(f"TUT:{tutorial_sessions-scheduled_tut}")
                    if scheduled_lab < lab_sessions: missing.append(f"LAB:{lab_sessions-scheduled_lab}")
                    if scheduled_ss < self_study: missing.append(f"SS:{self_study-scheduled_ss}")
                    
                    # Add row if there are missing components
                    if missing:
                        ws.append([
                            code,
                            name,
                            faculty,
                            ', '.join(required),
                            ', '.join(missing)
                        ])

                # Style the unscheduled courses section
                for row in ws.iter_rows(min_row=header_row, max_row=ws.max_row):
                    for cell in row:
                        cell.border = Border(left=Side(style='thin'), right=Side(style='thin'),
                                           top=Side(style='thin'), bottom=Side(style='thin'))
                        cell.alignment = Alignment(horizontal='center')

                # Adjust column widths for the unscheduled section
                for col in range(1, 6):
                    ws.column_dimensions[get_column_letter(col)].width = 20

    # Create unscheduled courses sheet
    if unscheduled_courses:
        ws_unscheduled = wb.create_sheet(title="Unscheduled Courses")
        
        # Add headers
        headers = ['Department', 'Semester', 'Course Code', 'Course Name', 'Faculty', 'Expected Slots', 'Scheduled Slots', 'Missing Slots']
        ws_unscheduled.append(headers)
        
        # Style headers
        for cell in ws_unscheduled[1]:
            cell.font = Font(bold=True)
            cell.alignment = Alignment(horizontal='center')
        
        # Add data
        for course in unscheduled_courses:
            ws_unscheduled.append([
                course['Department'],
                course['Semester'],
                course['Code'],
                course['Name'],
                course['Faculty'],
                course['Expected Slots'],
                course['Scheduled Slots'],
                course['Expected Slots'] - course['Scheduled Slots']
            ])
        
        # Style data cells
        for row in ws_unscheduled.iter_rows(min_row=2):
            for cell in row:
                cell.alignment = Alignment(horizontal='center')
        
        # Adjust column widths
        for column in ws_unscheduled.columns:
            max_length = 0
            column = list(column)
            for cell in column:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            ws_unscheduled.column_dimensions[get_column_letter(column[0].column)].width = max_length + 2

    # Save single workbook with error handling
    filename = "timetable_all.xlsx"
    try:
        # Try to save the file
        wb.save(filename)
        print(f"Complete timetable saved as {filename}")
    except PermissionError:
        # If file is open/locked, try saving with a new name
        import os
        base, ext = os.path.splitext(filename)
        counter = 1
        while True:
            new_filename = f"{base}_{counter}{ext}"
            try:
                wb.save(new_filename)
                print(f"File was locked. Saved as {new_filename} instead")
                filename = new_filename
                break
            except PermissionError:
                counter += 1
                if counter > 100:  # Prevent infinite loop
                    raise Exception("Unable to save file after 100 attempts")
    
    # Print unscheduled courses summary to console
    print_unscheduled_courses(unscheduled_courses)
    
    return [filename]

if __name__ == "__main__":
    generate_all_timetables()