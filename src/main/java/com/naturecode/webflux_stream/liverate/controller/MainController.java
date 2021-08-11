package com.naturecode.webflux_stream.liverate.controller;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class MainController {
  
  @GetMapping("/")
  public String index(final Model model) {
    return "index";
  }
}