require! {
  'prelude-ls': {flatten, camelize}
}

module.exports = (knex) ->
  return { create-route, chronological-list, has-many }

  function create-route table, keys, fkeys = [], polymorphic-fields = []
    keys ++= 'id'
    all-keys = keys ++ (fkeys.map (- /_id$/))
    routes = [
      route: "#{camelize table}ById[{integers}].#{JSON.stringify all-keys}"
      get: (path-set) ->
        # console.log table, (JSON.stringify path-set)
        keys = path-set.2
        ids = path-set.1

        q = knex table
          .where-in 'id', ids
          # .select ['id'] ++ keys # ++ fkeys # FIXME, should filter keys on path-set.2, beware off foreign keys
          .select!

        q
          .then (items) ->
            items-by-id = group-by-unique (.id), items
            ids.map (id) ->
              all-keys
                # .filter (in keys)
                .map (key) ->
                  if item = items-by-id[id]
                    if (key + '_id') in fkeys
                      value = item[key + '_id']
                      if value
                        value = {$type: 'ref', value: [(camelize key) + 'sById', value]}
                    else
                      value = item[key]
                      if typeof! value == 'Date'
                        value = value.toISOString!
                    # value = {$type: 'atom', $expires: -3600*1000, value}
                  else
                    value = {$type: 'error', value: 'not found'}
                  { path: ["#{camelize table}ById", id, key], value }
          .then flatten
          # .then (value) ->       {$type: 'atom', $expires: -1000, value}
          # .tap dump
    ]
    polymorphic-routes = polymorphic-fields.map (polymorphic-field) ->
      route: "#{camelize table}ById[{integers}].#{polymorphic-field}['type','item']"
      get: (path-set) ->
        # keys = path-set.2
        ids = path-set.1
        q = knex table
          .where-in 'id', ids
          .select "id", "#{polymorphic-field}_type", "#{polymorphic-field}_id"
          .then (items) ->
            items-by-id = group-by-unique (.id), items
            ids.map (id) ->
              item = items-by-id[id]
              type = item["#{polymorphic-field}_type"]
              [
                * path: ["#{camelize table}ById", id, polymorphic-field, "type"]
                  value: type
                * path: ["#{camelize table}ById", id, polymorphic-field, "item"]
                  value:
                    $type: 'ref'
                    value: [ "#{type}sById", item["#{polymorphic-field}_id"] ]
              ]
          .then flatten
    routes ++ polymorphic-routes


  function upsert table, data
    knex table
      .where data
      .select "#table.id"
      .first!
      .then (item) ->
        if item
          return {id: item.id, -inserted}
        knex table
          .insert data, \id
          .then ->
            {id: it.0, +inserted}

  function has-many table, has-many-table, selector, order-selector
    [
      * route: "#{table}ById[{integers:ids}].#{camelize has-many-table}[{ranges:range}]"
        get: (path-set) ->
          # console.log "has-many", table, has-many-table, selector, order-selector, path-set
          Promise.map path-set.ids, (id) ->
            Promise
              .map path-set.range, (range) ->
                q = knex has-many-table
                  .select 'id'
                  .offset range.from
                  .limit (range.to - range.from) + 1

                if selector
                  selector q, id

                if order-selector
                  order-selector q

                # console.log path-set
                # console.log q.to-string!

                q
                  .map (other-item, i) ->
                    path: [ "#{table}ById", id, (camelize has-many-table), range.from + i ]
                    value:
                      $type: 'ref'
                      value: [ "#{camelize has-many-table}ById", other-item.id ]
                  # .tap dump
          .then flatten

      * route: "#{table}ById[{integers:ids}].#{camelize has-many-table}.length"
        get: (path-set) ->
          Promise
            .map path-set.ids, (id) ->
              len = knex has-many-table
              if selector
                selector len, id
              len
                .count!
                .spread ->
                  [ { path: ["#{table}ById", id, (camelize has-many-table), 'length'], value: +it.count} ]
            .then flatten
            # .tap console.log
    ]

  function chronological-list table, selector, expires
    route: "#{camelize table}[{ranges}]"
    get: (path-set) ->
      range = path-set.1.0

      q = knex table
        .offset range.from
        .limit (range.to - range.from) + 1
        .select "#{table}.id"
        .order-by "#{table}.created_at", 'desc'

      if selector
        selector.call this, q

      q
        .map (item, i) ->
          value =
            $type: 'ref'
            value: ["#{camelize table}ById", item.id]

          if expires
            value.$expires = -expires

          path: [(camelize table), i + range.from]
          value: value

        # .tap dump

group-by-unique = (fn, items) -->
  {[(fn item), item] for item in items}
