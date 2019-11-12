const omitBy = require('lodash.omitby');
const { flatten, compose, defaultObj } = require('@keystonejs/utils');

/**
 * Format of input object:

  type Query {
    relationships: { <String>: Relationship },
    matchTerm: Object,
    postJoinPipeline: [ Object ],
  }

  type Relationship: {
    from: String,
    field: String,
    many?: Boolean, (default: true)
    ...Query,
  }

  eg;
  {
    relationships: {
      abc123: {
        from: 'posts-collection',
        field: 'posts',
        many: true,
        matchTerm: { title: { $eq: 'hello' } },
        postJoinPipeline: [
          { $limit: 10 },
        ],
        relationships: {
          ...
        },
      },
    },
    matchTerm: { $and: {
      { name: { $eq: 'foobar' } },
      { age: { $eq: 23 } },
      { abc123_posts_some: { $eq: true } }
    },
    postJoinPipeline: [
      { $orderBy: 'name' },
    ],
  }
 */
function mutation(uid, lookupPath) {
  return queryResult => {
    function mutate(arrayToMutate, lookupPathFragment, pathSoFar = []) {
      const [keyToMutate, ...restOfLookupPath] = lookupPathFragment;

      return arrayToMutate.map((value, index) => {
        if (!(keyToMutate in value)) {
          return value;
        }

        if (restOfLookupPath.length === 0) {
          // Now we can execute the mutation
          return omitBy(value, (_, keyToOmit) => keyToOmit.startsWith(uid));
        }

        // Recurse
        return {
          ...value,
          [keyToMutate]: mutate(value[keyToMutate], restOfLookupPath, [
            ...pathSoFar,
            index,
            keyToMutate,
          ]),
        };
      });
    }

    return mutate(queryResult, lookupPath);
  };
}

function mutationBuilder(relationships, path = []) {
  return compose(
    Object.entries(relationships).map(([uid, { relationshipInfo, relationships }]) => {
      const { uniqueField } = relationshipInfo;
      const postQueryMutations = mutationBuilder(relationships, [...path, uniqueField]);
      // NOTE: Order is important. We want depth first, so we perform the related mutators first.
      return compose([postQueryMutations, mutation(uid, [...path, uniqueField])]);
    })
  );
}

const lookupStage = ({ from, as, targetKey, foreignKey, extraPipeline = [] }) => ({
  $lookup: {
    from,
    as,
    let: { tmpVar: `$${targetKey}` },
    pipeline: [{ $match: { $expr: { $eq: [`$${foreignKey}`, '$$tmpVar'] } } }, ...extraPipeline],
  },
});

function relationshipPipeline(relationship) {
  const {
    from,
    fromTable,
    thisTable,
    rel,
    fromCollection,
    filterType,
    uniqueField,
  } = relationship.relationshipInfo;
  const { cardinality } = rel;
  let ret;
  const extraPipeline = pipelineBuilder(relationship);
  if (cardinality === '1:N' || cardinality === 'N:1') {
    let targetKey, foreignKey;
    if (rel.tableName === thisTable) {
      targetKey = rel.columnName;
      foreignKey = '_id';
    } else {
      targetKey = '_id';
      foreignKey = rel.columnName;
    }
    ret = [
      // Join against all the items which match the relationship filter condition
      lookupStage({ from, as: uniqueField, targetKey, foreignKey, extraPipeline }),
    ];
    if (filterType === 'every') {
      ret.push(
        // Match against *all* the items. Required for the _every condition below
        lookupStage({ from, as: `${uniqueField}_all`, targetKey, foreignKey, extraPipeline: [] })
      );
    }
  } else {
    const targetKey = '_id';
    const foreignKey = `${thisTable}_id`;
    ret = [
      lookupStage({
        from,
        as: uniqueField,
        targetKey,
        foreignKey,
        extraPipeline: [
          lookupStage({
            from: fromCollection,
            as: `${uniqueField}_0`,
            targetKey: `${fromTable}_id`,
            foreignKey: '_id',
            extraPipeline,
          }),
          { $match: { $expr: { $gt: [{ $size: `$${uniqueField}_0` }, 0] } } },
        ],
      }),
    ];
    if (filterType === 'every') {
      ret.push(
        lookupStage({
          from,
          as: `${uniqueField}_all`,
          targetKey,
          foreignKey,
          extraPipeline: [
            lookupStage({
              from: fromCollection,
              as: `${uniqueField}_0`,
              targetKey: `${fromTable}_id`,
              foreignKey: '_id',
              extraPipeline: [],
            }),
          ],
        })
      );
    }
  }
  return ret;
}

function pipelineBuilder({ relationships, matchTerm, excludeFields, postJoinPipeline }) {
  return [
    ...flatten(Object.values(relationships).map(relationshipPipeline)),
    matchTerm && { $match: matchTerm },
    { $addFields: { id: '$_id' } },
    excludeFields && excludeFields.length && { $project: defaultObj(excludeFields, 0) },
    ...postJoinPipeline, // $search / $orderBy / $skip / $first / $count
  ].filter(i => i);
}

module.exports = { pipelineBuilder, mutationBuilder };
