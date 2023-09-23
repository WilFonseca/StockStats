package org.challenge.api.controller

import org.challenge.service.StockService
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.{GetMapping, RestController}

@RestController
class StockController{

  @Autowired var stockService: StockService = _

  @GetMapping(path=Array("/stats"))
  def stats(): String = {
    var response: String = ""
    response += stockService.stockStats("IBM")
    response += stockService.stockStats("AAPL")
    response += stockService.stockStats("MSFT")
    response += stockService.stockStats("TSLA")

    response
  }
}
