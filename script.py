import sqlite3
conn = sqlite3.connect('data.sqlite')
pid=23597
rows = conn.execute("select PersonID, KnessetNum, DutyDesc, FactionName, CommitteeName, IsCurrent, LastUpdatedDate \
                    from person_to_position_raw where PersonID=?", (pid,)).fetchall()
print('raw count', len(rows))
# for r in rows:
#     # print(r)
agg = conn.execute("select PersonID, KnessetNum, DutyDesc, FactionName, CommitteeName, PositionIsCurrent, LastUpdatedMax \
                   from person_to_position_agg where PersonID=?", (pid,)).fetchall()
print('agg count', len(agg))
# for r in agg:
#     print(r)