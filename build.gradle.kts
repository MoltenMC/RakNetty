plugins {
    alias(libs.plugins.kotlin.jvm) apply false
    `maven-publish`
    signing
}

subprojects {
    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "maven-publish")
    apply(plugin = "signing")

    group   = "io.github.agent0876.raknetty"
    version = "0.1.0"

    repositories {
        mavenCentral()
    }

    dependencies {
        "testImplementation"(kotlin("test"))
    }

    tasks.withType<Test> {
        useJUnitPlatform()
    }

    extensions.configure<org.jetbrains.kotlin.gradle.dsl.KotlinJvmProjectExtension> {
        jvmToolchain(25)
    }

    // Sources & Javadoc JARs (Maven Central 필수 요건)
    val sourcesJar = tasks.register<Jar>("sourcesJar") {
        archiveClassifier.set("sources")
        from(project.the<SourceSetContainer>()["main"].allSource)
    }

    val javadocJar = tasks.register<Jar>("javadocJar") {
        archiveClassifier.set("javadoc")
        from(tasks.named("javadoc"))
    }

    extensions.configure<PublishingExtension> {
        publications {
            create<MavenPublication>("mavenJava") {
                from(components["java"])
                artifact(sourcesJar)
                artifact(javadocJar)

                pom {
                    name.set(project.name)
                    description.set("Netty-based RakNet protocol implementation in Kotlin")
                    url.set("https://github.com/Agent0876/RakNetty")

                    licenses {
                        license {
                            name.set("MIT License")
                            url.set("https://opensource.org/licenses/MIT")
                        }
                    }

                    developers {
                        developer {
                            id.set("agent0876")
                            name.set("Agent0876")
                            email.set("shinseungmin070920@gmail.com")
                        }
                    }

                    scm {
                        connection.set("scm:git:git://github.com/Agent0876/RakNetty.git")
                        developerConnection.set("scm:git:ssh://github.com/Agent0876/RakNetty.git")
                        url.set("https://github.com/Agent0876/RakNetty")
                    }
                }
            }
        }

        repositories {
            maven {
                name = "CentralPortal"
                // ⚠️ 주의: Central Portal 전용 주소로 Maven 배포를 시도할 때 404가 난다면 
                // 아래 S01 OSSRH 스테이징 주소를 사용해야 일반 maven-publish 플러그인이 정상 작동합니다.
                url = uri("https://s01.oss.sonatype.org/service/local/staging/deploy/maven2/")
                credentials {
                    username = System.getenv("SONATYPE_USERNAME")
                    password = System.getenv("SONATYPE_PASSWORD")
                }
            }
        }
    }

    extensions.configure<SigningExtension> {
        // Gradle 프로퍼티와 시스템 환경 변수를 모두 안전하게 조회하도록 개선
        val signingKeyId = (project.findProperty("signingKeyId") as? String) ?: System.getenv("ORG_GRADLE_PROJECT_signingKeyId") ?: System.getenv("SIGNING_KEY_ID")
        val signingKey = (project.findProperty("signingKey") as? String) ?: System.getenv("ORG_GRADLE_PROJECT_signingKey") ?: System.getenv("SIGNING_KEY")
        val signingPassword = (project.findProperty("signingPassword") as? String) ?: System.getenv("ORG_GRADLE_PROJECT_signingPassword") ?: System.getenv("SIGNING_PASSWORD")

        if (!signingKey.isNullOrBlank() && !signingPassword.isNullOrBlank()) {
            // 인메모리 서명 시 Key ID를 명시적으로 넘겨주어야 무한 행(Hang) 현상을 방지할 수 있습니다.
            useInMemoryPgpKeys(signingKeyId, signingKey, signingPassword)
            sign(extensions.getByType<PublishingExtension>().publications["mavenJava"])
        } else {
            logger.warn("[$name] Signing credentials not found. Skipping signature.")
        }
    }
}