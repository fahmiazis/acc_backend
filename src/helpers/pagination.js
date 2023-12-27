const qs = require('querystring')
// const { APP_URL, APP_PORT } = process.env

module.exports = {
  pagination: (api, query, page, limit, count) => {
    page = +page
    const pageInfo = {
      count: 0,
      pages: 0,
      currentPage: +page,
      limitPerPage: +limit,
      nextLink: null,
      prevLink: null
    }

    pageInfo.count = count
    pageInfo.pages = Math.ceil(count / limit)
    const { pages, currentPage } = pageInfo
    if (currentPage < pages) {
      pageInfo.nextLink = `${api}?${qs.stringify({ ...query, ...{ page: page + 1 } })}`
    }

    if (currentPage > 1) {
      pageInfo.prevLink = `${api}?${qs.stringify({ ...query, ...{ page: page - 1 } })}`
    }
    return pageInfo
  }
}
