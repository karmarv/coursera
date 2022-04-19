
# prep
CREATE CONSTRAINT ON (u:User) ASSERT u.id IS UNIQUE;
CREATE CONSTRAINT ON (t:Team) ASSERT t.id IS UNIQUE;
CREATE CONSTRAINT ON (c:TeamChatSession) ASSERT c.id IS UNIQUE;
CREATE CONSTRAINT ON (i:ChatItem) ASSERT i.id IS UNIQUE;

# 0 chat_create_team_chat.csv
LOAD CSV FROM "file:///home/rahul/workspace/coursera-sdsc-words/coursera/big-data-capstone/chat-data/chat_create_team_chat.csv" AS row
MERGE (u:User {id: toInt(row[0])})
MERGE (t:Team {id: toInt(row[1])})
MERGE (c:TeamChatSession {id: toInt(row[2])})
MERGE (u)-[:CreatesSession{timeStamp: row[3]}]->(c)
MERGE (c)-[:OwnedBy{timeStamp: row[3]}]->(t);


# (i) chat_join_team_chat.csv
LOAD CSV FROM "file:///home/rahul/workspace/coursera-sdsc-words/coursera/big-data-capstone/chat-data/chat_join_team_chat.csv" AS row
MERGE (u:User {id: toInt(row[0])})
MERGE (c:TeamChatSession {id: toInt(row[1])})
MERGE (u)-[:Joins{timeStamp: row[2]}]->(c);

# (ii) chat_leave_team_chat.csv
LOAD CSV FROM "file:///home/rahul/workspace/coursera-sdsc-words/coursera/big-data-capstone/chat-data/chat_leave_team_chat.csv" AS row
MERGE (u:User {id: toInt(row[0])})
MERGE (c:TeamChatSession {id: toInt(row[1])})
MERGE (u)-[:Leaves{timeStamp: row[2]}]->(c);

# (iii) chat_item_team_chat.csv
LOAD CSV FROM "file:///home/rahul/workspace/coursera-sdsc-words/coursera/big-data-capstone/chat-data/chat_item_team_chat.csv" AS row
MERGE (u:User {id: toInt(row[0])})
MERGE (c:TeamChatSession {id: toInt(row[1])})
MERGE (i:ChatItem {id: toInt(row[2])})
MERGE (u)-[:CreateChat{timeStamp: row[3]}]->(c)
MERGE (i)-[:PartOf{timeStamp: row[3]}]->(c);

# (iv) chat_mention_team_chat.csv 
LOAD CSV FROM "file:///home/rahul/workspace/coursera-sdsc-words/coursera/big-data-capstone/chat-data/chat_mention_team_chat.csv" AS row
MERGE (i:ChatItem {id: toInt(row[0])})
MERGE (u:User {id: toInt(row[1])})
MERGE (i)-[:Mentioned{timeStamp: row[2]}]->(u);

# (v) chat_respond_team_chat.csv 
LOAD CSV FROM "file:///home/rahul/workspace/coursera-sdsc-words/coursera/big-data-capstone/chat-data/chat_respond_team_chat.csv" AS row
MERGE (i:ChatItem {id: toInt(row[0])})
MERGE (j:ChatItem {id: toInt(row[1])})
MERGE (j)-[:ResponseTo{timeStamp: row[2]}]->(i);


# -------------------------------------------------------------------------------------- #


# Q1 -- Find the longest conversation chain in the chat data using the "ResponseTo" edge label. 

MATCH p=(a)-[:ResponseTo*]->(c) WHERE a.id <> c.id RETURN p, length(p) ORDER BY length(p) DESC LIMIT 1;
MATCH p=(a)-[:ResponseTo*]->(b) WHERE length(p) = 9 WITH p MATCH (u:User)-[]-(d) WHERE d IN NODES(p) RETURN DISTINCT u;

MATCH p=(c:ChatItem)-[:ResponseTo*]->(d:ChatItem) WHERE length(p)=9 WITH p MATCH q=(u:User)-[]-(c:ChatItem) WHERE (c IN NODES(p)) RETURN COUNT(DISTINCT u);

# Q2 -- Do the top 10 the chattiest users belong to the top 10 chattiest teams? 

MATCH (u:User)-[:CreateChat]->() RETURN u.id AS userID, COUNT(u) ORDER BY COUNT(u) DESC LIMIT 10;
MATCH (i:ChatItem)-[:PartOf]->(c:TeamChatSession)-[:OwnedBy]->(t:Team) RETURN t.id AS teamId, COUNT(i)  ORDER BY COUNT(i) DESC LIMIT 10;

MATCH (u:User)-[r:CreateChat]->(:TeamChatSession)-[:OwnedBy]->(t:Team) RETURN u.id AS userID, t.id as teamId, COUNT(r) AS outdegree ORDER BY outdegree DESC LIMIT 10;

# Q3 -- # How Active are Groups of Users?

MATCH (u1:User)-[:CreateChat]->(c:TeamChatSession)<-[:PartOf]-(i:ChatItem)-[:Mentioned]->(u2:User) CREATE (u1)-[:InteractsWith]->(u2);
MATCH (u1:User)-[:CreateChat]->(c1:TeamChatSession)<-[:PartOf]-(i1:ChatItem)<-[:ResponseTo]-(i2:ChatItem)-[:PartOf]->(c2:TeamChatSession)<-[:CreateChat]-(u2:User) CREATE (u1)-[:InteractsWith]->(u2);
MATCH (u1)-[r:InteractsWith]->(u1) DELETE r;

MATCH (u1:User{id:1192})-[iw1:InteractsWith]->(u2:User)
WITH COLLECT(u2.id) as neighbours, COUNT(u2) AS k
MATCH (u3:User)-[iw2:InteractsWith]->(u4:User)
WHERE (u3.id in (neighbours)) AND (u4.id in (neighbours)) AND (u3.id <> u4.id)
WITH COUNT(iw2) AS numerator, (k * (k - 1) * 1.0) AS denominator
RETURN numerator/denominator AS clusterCoefficient;

# -------------------------------------------------------------------------------------- #