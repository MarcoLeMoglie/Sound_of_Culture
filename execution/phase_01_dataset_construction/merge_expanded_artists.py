
import json
import os

seed_file = "data/phase_01_dataset_construction/intermediate/json/seed_country_artists.json"
new_artists = [
  "Alabama", "Alan Jackson", "Appalatin", "Arum Rae", "Bailey Zimmerman", "Bellamy Brothers", "Beyoncé", "Bill Anderson", "Billy Currington", "Billy Don Burns", "Billy Grammer", "Blanco Brown", "Blake Shelton", "Bobby Bare", "Boxcar Willie", "Brad Paisley", "Breland", "Brett Eldredge", "Brett Young", "Brooks & Dunn", "Buckwheat Zydeco", "Carly Pearce", "Carrie Underwood", "Chris Janson", "Chris Knight", "Chris Stapleton", "Chris Young", "Christian Lopez", "Clint Black", "Cody Johnson", "Cole Swindell", "Connor Smith", "Dan + Shay", "Dave Dudley", "Dierks Bentley", "Diplo", "Dixie Chicks", "Dolly Parton", "Drew Baldridge", "Dustin Lynch", "Dylan Scott", "Earl Scruggs", "Easton Corbin", "Evan Bartels", "Faith Hill", "Florida Georgia Line", "Gabby Barrett", "Garth Brooks", "Gary Mule Deer", "George Burns", "George Hamilton IV", "George Strait", "Glen Campbell", "Grayson Capps", "Gwen Stefani", "Hailey Whitters", "Hank Snow", "Hardy", "Henry Cho", "Jake Owen", "James & Michael Younger", "James Maddock", "Jameson Rodgers", "Jamie O'Neal", "Jason Aldean", "Jelly Roll", "Jessie Murph", "Joe Nichols", "Joe Stampley", "John Conlee", "John Rich", "Johnny Cash", "Jon Pardi", "Jordan Davis", "Josh Turner", "Kane Brown", "Kate Mann", "Kathy Mattea", "Keith Urban", "Keith Whitley", "Kenny Chesney", "Kenny Rogers", "Kentucky Headhunters", "Kix Brooks", "Lacy J. Dalton", "Lady Antebellum", "Lainey Wilson", "Lee Brice", "Libbi Bosworth", "Libby Johnson", "Lilli Lewis", "Little Big Town", "Lonestar", "Lorrie Morgan", "Luke Bryan", "Luke Combs", "Mac Davis", "Maggie Antone", "Maren Morris", "Marisa Anderson", "Marty Robbins", "Marty Stuart", "Mary Chapin Carpenter", "Minnie Pearl", "Mitch Zorn", "Mitchell Tenpenny", "Moe Bandy", "Moondi Klein", "Morgan Wallen", "Old Dominion", "Parker McCollum", "Parmalee", "Patty Loveless", "Phil Vassar", "Polo G", "Porter Wagoner", "Post Malone", "Randy Houser", "Randy Travis", "Rascal Flatts", "Raul Malo", "Razzy Bailey", "Reba McEntire", "Ricky Van Shelton", "Riley Green", "Rodney Crowell", "Ronnie Milsap", "Rosanne Cash", "Roy Acuff", "Russell Dickerson", "Sam Hunt", "Shania Twain", "Shannon LaBrie", "Sierra Ferrell", "Sissy Spacek", "Steel Magnolia", "Tanya Tucker", "Taylor Swift", "The Mavericks", "The War and Treaty", "Thomas Rhett", "Tim McGraw", "Toby Keith", "Tom T. Hall", "Travis Tritt", "Trick Pony", "Trixie Mattel", "Tyler Hubbard", "Vince Gill", "Walker Hayes", "Warren Zeiders", "Warren Zevon", "Zac Brown Band", "Zaca Creek"
]

if os.path.exists(seed_file):
    with open(seed_file, 'r') as f:
        existing_artists = json.load(f)
else:
    existing_artists = []

total_artists = sorted(list(set(existing_artists + new_artists)))

with open(seed_file, 'w') as f:
    json.dump(total_artists, f, indent=4)

print(f"Updated {seed_file}. Total unique artists: {len(total_artists)}")
