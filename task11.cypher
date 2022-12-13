MATCH (s:Sequence)
WHERE s.regionCode = 'CZ' OR s.countryCode = 'UKR'
WITH count(s) AS size
CREATE (task:Task {name: 'BaltoSlavic, Ukraine (233) [UKR_CZ]', region:'UKR_CZ', count: size});

// RSRS
MATCH (t:Task {region:'UKR_CZ'})
CREATE (t)-[:RSRS]->(rsrs:DataNode {type:'rsrs'});

// Sample
MATCH (s:Sequence)
WHERE s.regionCode = 'CZ' OR s.countryCode = 'UKR'
// get RSRS
CALL {
    WITH s
    MATCH (brs:BasicSequence {id:'RSRS'})
    RETURN apoc.text.hammingDistance(substring(s.sequence, 27, 312), substring(brs.sequence, 27, 312)) AS rsrs_distances
}

MATCH (task:Task {region:'UKR_CZ'})-[:RSRS]->(d:DataNode)

WITH collect(rsrs_distances) AS distances, d
// MAX, MIN, STDEV
WITH apoc.coll.max(distances) AS max, apoc.coll.min(distances) AS min, apoc.coll.stdev(distances) AS stdev, distances,d
SET d.max = max, d.min=min, d.stdev = stdev

WITH apoc.coll.frequencies(distances) AS frequencies, distances

CALL { 
    WITH frequencies, distances
    UNWIND frequencies AS fr
    RETURN toFloat(fr.count)/size(distances) AS percent, fr.count AS amount, fr.item AS distance
}

with percent, amount, distance

MATCH (task:Task {region:'UKR_CZ'})-[:RSRS]->(rsrs:DataNode {type:'rsrs'})
CREATE (rsrs)<-[:HAS]-(d:DistanceNode {distance:distance, percent:percent, amount:amount}); 

MATCH (task:Task {region:'UKR_CZ'})-[:RSRS]->(rsrs:DataNode)<-[:HAS]-(d:DistanceNode)
WITH sum(d.percent * d.distance) AS expected_value, rsrs
WITH rsrs.stdev/expected_value AS variation_coef, expected_value, rsrs 
SET rsrs.expected_value = expected_value, rsrs.variation_coef = variation_coef
RETURN rsrs;

CALL {
	MATCH (task:Task {region:'UKR_CZ'})-[:RSRS]->(rsrs:DataNode)<-[:HAS]-(d:DistanceNode)
	RETURN d.distance AS moda, rsrs
	ORDER BY d.amount DESC
	LIMIT 1
}
SET rsrs.moda = moda
RETURN rsrs;


// rCSRS
MATCH (t:Task {region:'UKR_CZ'})
CREATE (t)-[:RCRS]->(rcrs:DataNode {type:'rcrs'});

// Sample
MATCH (s:Sequence)
WHERE s.regionCode = 'CZ' OR s.countryCode = 'UKR'
// get RSRS
CALL {
    WITH s
    MATCH (brs:BasicSequence {id:'rCRS'})
    RETURN apoc.text.hammingDistance(substring(s.sequence, 27, 312), substring(brs.sequence, 27, 312)) AS rcrs_distances
}

MATCH (task:Task {region:'UKR_CZ'})-[:RCRS]->(d:DataNode)

WITH collect(rcrs_distances) AS distances, d
// MAX, MIN, STDEV
WITH apoc.coll.max(distances) AS max, apoc.coll.min(distances) AS min, apoc.coll.stdev(distances) AS stdev, distances,d 
SET d.max = max, d.min=min, d.stdev = stdev

WITH apoc.coll.frequencies(distances) AS frequencies, distances

CALL { 
    WITH frequencies, distances
    UNWIND frequencies AS fr
    RETURN toFloat(fr.count)/size(distances) AS percent, fr.count AS amount, fr.item AS distance
}

with percent, amount, distance

MATCH (task:Task {region:'UKR_CZ'})-[:RCRS]->(rcrs:DataNode {type:'rcrs'})
CREATE (rcrs)<-[:HAS]-(d:DistanceNode {distance:distance, percent:percent, amount:amount}); 

MATCH (task:Task {region:'UKR_CZ'})-[:RCRS]->(rcrs:DataNode)<-[:HAS]-(d:DistanceNode)
WITH sum(d.percent * d.distance) AS expected_value, rcrs
WITH rcrs.stdev/expected_value AS variation_coef, expected_value, rcrs 
SET rcrs.expected_value = expected_value, rcrs.variation_coef = variation_coef
RETURN rcrs;

CALL {
	MATCH (task:Task {region:'UKR_CZ'})-[:RCRS]->(rcrs:DataNode)<-[:HAS]-(d:DistanceNode)
	RETURN d.distance AS moda, rcrs
	ORDER BY d.amount DESC
	LIMIT 1
}
SET rcrs.moda = moda
RETURN rcrs;


// WILDTYPE
MATCH (t:Task {region:'UKR_CZ'})
CREATE (t)-[:WILDTYPE]->(wt:DataNode {type:'wildtype'});

UNWIND range(0, 311) AS i
CALL {
    WITH i
		MATCH (seq:Sequence)
		WHERE seq.regionCode = 'CZ' OR seq.countryCode = 'UKR'
    RETURN apoc.coll.max(apoc.coll.frequencies(collect(apoc.text.code(apoc.text.charAt(substring(seq.sequence, 27, 312), i))))).item AS common
}
WITH apoc.text.join(collect(common), '') AS wt_seq
MATCH (t:Task {region:'UKR_CZ'})-[:WILDTYPE]->(wt:DataNode)
SET wt.wildtype = wt_seq;


// Sample
MATCH (s:Sequence)
WHERE s.regionCode = 'CZ' OR s.countryCode = 'UKR'
// get RSRS
CALL {
    WITH s
    MATCH (t:Task {region:'UKR_CZ'})-[:WILDTYPE]->(wt:DataNode)
    RETURN apoc.text.hammingDistance(substring(s.sequence, 27, 312), wt.wildtype) AS wt_distances
}


MATCH (task:Task {region:'UKR_CZ'})-[:WILDTYPE]->(d:DataNode)

WITH collect(wt_distances) AS distances, d
// MAX, MIN, STDEV
WITH apoc.coll.max(distances) AS max, apoc.coll.min(distances) AS min, apoc.coll.stdev(distances) AS stdev, distances,d 
SET d.max = max, d.min=min, d.stdev = stdev

WITH apoc.coll.frequencies(distances) AS frequencies, distances

CALL { 
    WITH frequencies, distances
    UNWIND frequencies AS fr
    RETURN toFloat(fr.count)/size(distances) AS percent, fr.count AS amount, fr.item AS distance
}

with percent, amount, distance

MATCH (task:Task {region:'UKR_CZ'})-[:WILDTYPE]->(rcrs:DataNode {type:'wildtype'})
CREATE (rcrs)<-[:HAS]-(d:DistanceNode {distance:distance, percent:percent, amount:amount}); 

MATCH (task:Task {region:'UKR_CZ'})-[:WILDTYPE]->(rcrs:DataNode)<-[:HAS]-(d:DistanceNode)
WITH sum(d.percent * d.distance) AS expected_value, rcrs
WITH rcrs.stdev/expected_value AS variation_coef, expected_value, rcrs 
SET rcrs.expected_value = expected_value, rcrs.variation_coef = variation_coef
RETURN rcrs;

CALL {
	MATCH (task:Task {region:'UKR_CZ'})-[:WILDTYPE]->(rcrs:DataNode)<-[:HAS]-(d:DistanceNode)
	RETURN d.distance AS moda, rcrs
	ORDER BY d.amount DESC
	LIMIT 1
}
SET rcrs.moda = moda
RETURN rcrs;


// Pairs
MATCH (t:Task {region:'UKR_CZ'})
CREATE (t)-[:PAIR]->(n:DataNode {type:'pair'});

CALL {
    MATCH (a:Sequence), (b:Sequence)
		WHERE a.id < b.id AND (a.regionCode = 'CZ' OR a.countryCode = 'UKR') AND (b.regionCode = 'CZ' OR b.countryCode = 'UKR')
		RETURN apoc.text.hammingDistance(substring(a.sequence, 27, 312), substring(b.sequence, 27, 312)) AS pair_distances
}

MATCH (task:Task {region:'UKR_CZ'})-[:PAIR]->(d:DataNode)

WITH collect(pair_distances) AS distances, d
// MAX, MIN, STDEV
WITH apoc.coll.max(distances) AS max, apoc.coll.min(distances) AS min, apoc.coll.stdev(distances) AS stdev, distances,d 
SET d.max = max, d.min=min, d.stdev = stdev

WITH apoc.coll.frequencies(distances) AS frequencies, distances

CALL { 
    WITH frequencies, distances
    UNWIND frequencies AS fr
    RETURN toFloat(fr.count)/size(distances) AS percent, fr.count AS amount, fr.item AS distance
}

with percent, amount, distance

MATCH (task:Task {region:'UKR_CZ'})-[:PAIR]->(n:DataNode {type:'pair'})
CREATE (n)<-[:HAS]-(d:DistanceNode {distance:distance, percent:percent, amount:amount}); 

MATCH (task:Task {region:'UKR_CZ'})-[:PAIR]->(n:DataNode)<-[:HAS]-(d:DistanceNode)
WITH sum(d.percent * d.distance) AS expected_value, n
WITH n.stdev/expected_value AS variation_coef, expected_value, n 
SET n.expected_value = expected_value, n.variation_coef = variation_coef
RETURN n;

CALL {
	MATCH (task:Task {region:'UKR_CZ'})-[:PAIR]->(n:DataNode)<-[:HAS]-(d:DistanceNode)
	RETURN d.distance AS moda, n
	ORDER BY d.amount DESC
	LIMIT 1
}
SET n.moda = moda
RETURN n;

MATCH (task:Task {region:'UKR_CZ'})-[:WILDTYPE]->(d:DataNode)
MATCH (brs:BasicSequence {id:'RSRS'})
MATCH (brc:BasicSequence {id:'rCRS'})
SET d.amount_rsrs = apoc.text.hammingDistance(d.wildtype, substring(brs.sequence, 27, 312)), d.amount_rcrs = apoc.text.hammingDistance(d.wildtype, substring(brc.sequence, 27, 312));

MATCH (task:Task {region:'UKR_CZ'})-[:RCRS]->(rcrs:DataNode)<-[:HAS]-(d1:DistanceNode)
WITH sum(d1.distance) AS amo, rcrs
SET rcrs.amount=amo;

MATCH (task:Task {region:'UKR_CZ'})-[:RSRS]->(rsrs:DataNode)<-[:HAS]-(d1:DistanceNode)
WITH sum(d1.distance) AS amo, rsrs
SET rsrs.amount=amo;

// Haplogroups
MATCH (task:Task {region:'UKR_CZ'}), (seq:Sequence)
WHERE seq.regionCode = 'CZ' OR seq.countryCode = 'UKR'
WITH seq.haplogroup AS hg, count(seq.haplogroup) AS cnt, task
WITH collect(hg) AS hgs, collect(cnt) AS cnts, task
SET task.haplogroups = hgs, task.haplogroupCounts = cnts;

// Population mutations
MATCH (bs:BasicSequence {id:'rCRS'}), (s:Sequence)
WHERE s.regionCode = 'CZ' OR s.countryCode = 'UKR'
WITH collect(bs.sequence)[0] AS bseq, collect(s.sequence) as seqs
UNWIND range(0, 311) AS i
WITH apoc.coll.occurrences(collect(any(seq IN seqs WHERE apoc.text.charAt(substring(seq, 27, 312), i) <> apoc.text.charAt(substring(bseq, 27, 312), i))), true) AS popMutations
MATCH (:Task {region:'UKR_CZ'})-[:RCRS]->(d:DataNode)
SET d.popMutations = popMutations;

MATCH (bs:BasicSequence {id:'RSRS'}), (s:Sequence)
WHERE s.regionCode = 'CZ' OR s.countryCode = 'UKR'
WITH collect(bs.sequence)[0] AS bseq, collect(s.sequence) as seqs
UNWIND range(0, 311) AS i
WITH apoc.coll.occurrences(collect(any(seq IN seqs WHERE apoc.text.charAt(substring(seq, 27, 312), i) <> apoc.text.charAt(substring(bseq, 27, 312), i))), true) AS popMutations
MATCH (:Task {region:'UKR_CZ'})-[:RSRS]->(d:DataNode)
SET d.popMutations = popMutations;