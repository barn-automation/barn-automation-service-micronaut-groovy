package codes.recursive.controller


import groovy.json.JsonGenerator
import groovy.transform.CompileStatic
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.http.annotation.Produces

@CompileStatic
@Controller("/")
class HomeController {

    HomeController() {
    }

    @Get("/")
    @Produces(MediaType.APPLICATION_JSON)
    String home() {
        return new JsonGenerator.Options().build().toJson(
                [
                        health: "OK",
                        at: new Date(),
                        source: "micronaut-groovy",
                ]
        )
    }
}
