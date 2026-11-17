# Intel ISA-L erasure_code only (C + optional ARM NEON).
include_guard(GLOBAL)

set(WINJECT_FEC_ISAL_EC_DIR "${CMAKE_CURRENT_LIST_DIR}")
enable_language(C)
if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64|ARM64")
    enable_language(ASM)
endif()

set(WINJECT_FEC_ISAL_PIN 41a840a784040dc68b5363c0a6280e796382b707)

# Downloads ISA-L once; sets WINJECT_FEC_ISAL_ROOT in the caller's scope.
macro(winject_fec_fetch_isal)
    include(FetchContent)
    FetchContent_Declare(isa-l
        GIT_REPOSITORY https://github.com/intel/isa-l.git
        GIT_TAG ${WINJECT_FEC_ISAL_PIN}
    )
    FetchContent_GetProperties(isa-l)
    if(NOT isa-l_POPULATED)
        FetchContent_Populate(isa-l)
    endif()
    set(WINJECT_FEC_ISAL_ROOT "${isa-l_SOURCE_DIR}")
endmacro()

# Static target winject_fec_isal: ec_base.c, plus the NEON dot-product kernels on aarch64
# (then it exports WINJECT_FEC_ISAL_NEON=1 to whatever links it).
function(winject_fec_add_isal)
    if(TARGET winject_fec_isal)
        return()
    endif()
    winject_fec_fetch_isal()
    set(ISAL_ROOT "${WINJECT_FEC_ISAL_ROOT}")
    set(ISAL_EC_SOURCES "${ISAL_ROOT}/erasure_code/ec_base.c")
    set(ISAL_EC_NEON OFF)

    if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64|ARM64")
        set(ISAL_EC_NEON ON)
        file(MAKE_DIRECTORY "${ISAL_ROOT}/erasure_code/include")
        file(COPY "${ISAL_ROOT}/include/aarch64_label.h"
             DESTINATION "${ISAL_ROOT}/erasure_code/include")
        list(APPEND ISAL_EC_SOURCES
            "${WINJECT_FEC_ISAL_EC_DIR}/isal_ec_neon.c"
            "${ISAL_ROOT}/erasure_code/aarch64/gf_vect_dot_prod_neon.S"
            "${ISAL_ROOT}/erasure_code/aarch64/gf_2vect_dot_prod_neon.S"
            "${ISAL_ROOT}/erasure_code/aarch64/gf_3vect_dot_prod_neon.S"
            "${ISAL_ROOT}/erasure_code/aarch64/gf_4vect_dot_prod_neon.S"
            "${ISAL_ROOT}/erasure_code/aarch64/gf_5vect_dot_prod_neon.S"
        )
    endif()

    add_library(winject_fec_isal STATIC ${ISAL_EC_SOURCES})
    set_target_properties(winject_fec_isal PROPERTIES POSITION_INDEPENDENT_CODE ON)
    target_include_directories(winject_fec_isal
        PUBLIC
            "${ISAL_ROOT}/include"
        PRIVATE
            "${ISAL_ROOT}"
            "${ISAL_ROOT}/erasure_code"
    )
    target_compile_definitions(winject_fec_isal PRIVATE NDEBUG)
    if(ISAL_EC_NEON)
        target_compile_definitions(winject_fec_isal PUBLIC WINJECT_FEC_ISAL_NEON=1)
    endif()
endfunction()
