require! {
  'prelude-ls': {flatten, camelize, unique}
}

module.exports = (knex) ->
  return { create-route, chronological-list, has-many }

  function create-route collection-name, keys, opts = {}
    fkeys = opts.fkeys or []
    polymorphic-fields = opts.polymorphic-fields or []
    table = opts.table or collection-name
    collection-name = camelize collection-name
    # console.log {collection-name, keys, table, fkeys, polymorphic-fields}
    keys ++= 'id'
    all-keys = keys ++ (fkeys.map (- /_id$/)) |> unique
    routes = [
      route: "#{collection-name}ById[{integers}].#{JSON.stringify all-keys}"
      get: (path-set) ->
        # console.log table, (JSON.stringify path-set)
        keys = path-set.2
        ids = path-set.1

        q = knex table
          .where-in 'id', ids
          # .select ['id'] ++ keys # ++ fkeys # FIXME, should filter keys on path-set.2, beware of foreign keys
          .select!

        if opts.filter
          opts.filter q

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
                      # console.log item, key
                      value = item[key]
                      if typeof! value == 'Date'
                        value = value.toISOString!
                      else if key == 'id'
                        value = +value

                      if value == void
                        value = {$type: 'atom'}
                    # value = {$type: 'atom', $expires: -3600*1000, value}
                  else
                    value = {$type: 'error', value: 'not found'}
                  { path: ["#{camelize table}ById", id, key], value }
          .then flatten
          # .then (value) ->       {$type: 'atom', $expires: -1000, value}
          # .tap dump
    ]

    polymorphic-routes = polymorphic-fields.map (polymorphic-field) ->
      route: "#{collection-name}ById[{integers}].#{polymorphic-field}['type','item']"
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

  function chronological-list collection-name, opts = {}
    table = opts.table or collection-name
    collection-name = if opts.table then collection-name else camelize table
    order-by = opts.order-by or 'created_at'

    route: "#{collection-name}[{ranges}]"
    get: (path-set) ->
      range = path-set.1.0

      q = knex table
        .offset range.from
        .limit (range.to - range.from) + 1
        .select "#{table}.id"
        .order-by "#{table}.#{order-by}", 'desc'

      if opts.filter?
        opts.filter.call this, q

      q
        .map (item, i) ->
          value =
            $type: 'ref'
            value: ["#{collection-name}ById", item.id]

          if opts.expires
            value.$expires = -opts.expires

          path: [collection-name, i + range.from]
          value: value

        # .tap dump

function group-by-unique fn, items
  {[(fn item), item] for item in items}

function dump response
  console.log JSON.stringify(response, null, 2)
