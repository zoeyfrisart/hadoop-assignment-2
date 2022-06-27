-- Load the data using pig storage
ordersData = LOAD '/user/maria_dev/diplomacy/orders.csv'
  USING PigStorage(',')
  AS (game_id:chararray,
    unit_id:chararray,
    unit_order:chararray,
    location:chararray,
    target:chararray,
    target_dest:chararray,
    success:chararray,
    reason:chararray,
    turn_num:chararray);

-- Filter the results to only include the orders that had a target destination for Holland.
-- We put "" around Holland since the order.csv contains a "" around the target.
filteredResults = FILTER ordersData BY target == '"Holland"';

-- Create a group that contains the location that performed the action and the target of the action.
groupFilteredResults = GROUP filteredResults BY (location, target);

-- Count how many times each location targetted Holland.
countLocation = FOREACH groupFilteredResults GENERATE group, COUNT(filteredResults);

-- Sort the results so that the locations are in alphabetical order.
sortedResults = ORDER countLocation BY $0 ASC;

-- Show the results to the end user
DUMP sortedResults;
